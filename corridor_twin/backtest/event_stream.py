"""事件流与决策时点可见性视图。

事件流是不可变的有序事件集合。同一旅程的同一观测点若事后被更正，
会以更大的 revision 追加一条 is_correction=true 的事件；回测引擎按
occurred_at 顺序重放，并在每个决策时刻只看见当时已经到达的事件版本，
从而杜绝用后来修订的路况、容量美化历史成绩。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping

from corridor_twin.events.logistics_event import (
    ContractError,
    DomainRecord,
    canonical_json,
    finite_number,
    normalized_text,
    stable_hash,
)

OBSERVATION = "observation"
OUTCOME = "outcome"
CORRECTION_KIND = "correction"
EVENT_KINDS = (OBSERVATION, OUTCOME)

# 旅程异常结局；这些旅程必须保留在统计口径内。
UNREACHABLE = "unreachable"
REROUTED = "rerouted"
COLD_CHAIN_EXCEPTION = "cold_chain_exception"
DELIVERED = "delivered"
OUTCOME_STATES = (DELIVERED, UNREACHABLE, REROUTED, COLD_CHAIN_EXCEPTION)
EXCEPTION_STATES = (UNREACHABLE, REROUTED, COLD_CHAIN_EXCEPTION)


def parse_event_time(value: Any) -> datetime:
    """解析带时区的 ISO-8601 时间；无时区按 UTC 处理。"""
    text = normalized_text(value).upper()
    if not text:
        raise ContractError("事件缺少 occurred_at")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    from datetime import timezone
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ContractError(f"无法解析事件时间: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def route_parts(route_code: Any) -> tuple[str, ...]:
    route = normalized_text(route_code).replace("_", "-")
    parts = tuple(part for part in route.split("-") if part)
    if len(parts) < 2:
        raise ContractError(f"route_code 无法解析为连续路径: {route_code!r}")
    return parts


def derive_border(route_code: Any) -> str:
    """口岸取路径上第二个节点（起运地-口岸-目的地）；两节点路径取终点。"""
    parts = route_parts(route_code)
    return parts[1] if len(parts) >= 3 else parts[-1]


@dataclass(frozen=True)
class StreamEvent:
    shipment_id: str
    kind: str
    occurred_at: str
    revision: int
    payload: dict[str, Any]
    state: str = "booked"
    is_correction: bool = False
    event_sequence: int = 0
    record_id: str = ""
    source: str = "event-stream"
    observed_at: str = ""

    def __post_init__(self) -> None:
        if self.kind not in EVENT_KINDS:
            raise ContractError(f"未知事件类型: {self.kind!r}")
        if self.revision < 1:
            raise ContractError("事件版本号必须为正整数")
        parse_event_time(self.occurred_at)
        if not self.shipment_id.strip():
            raise ContractError("事件缺少 shipment_id")
        if self.kind == OUTCOME and self.state not in OUTCOME_STATES:
            raise ContractError(f"结局事件状态非法: {self.state!r}")
        if not self.record_id:
            object.__setattr__(self, "record_id",
                               stable_hash({"shipment_id": self.shipment_id, "kind": self.kind,
                                            "revision": self.revision, "occurred_at": self.occurred_at})[:20])
        if not self.observed_at:
            object.__setattr__(self, "observed_at", self.occurred_at)

    @property
    def time(self) -> datetime:
        return parse_event_time(self.occurred_at)

    @property
    def observed_time(self) -> datetime:
        return parse_event_time(self.observed_at)

    @property
    def is_exception(self) -> bool:
        return self.kind == OUTCOME and self.state in EXCEPTION_STATES

    @property
    def fingerprint(self) -> str:
        return stable_hash(self.as_dict())

    def as_dict(self) -> dict[str, Any]:
        return {"shipment_id": self.shipment_id, "kind": self.kind, "occurred_at": self.occurred_at,
                "revision": self.revision, "state": self.state, "is_correction": self.is_correction,
                "event_sequence": self.event_sequence, "record_id": self.record_id,
                "source": self.source, "observed_at": self.observed_at, "payload": self.payload}


def _coerce_event(raw: Mapping[str, Any], index: int) -> StreamEvent:
    if not isinstance(raw, Mapping):
        raise ContractError(f"事件流第 {index} 项必须是对象")
    kind = normalized_text(raw.get("kind") or OBSERVATION)
    shipment_id = str(raw.get("shipment_id") or "").strip()
    if not shipment_id:
        # 兼容 DomainRecord 风格：从 payload 取 shipment_id。
        shipment_id = str((raw.get("payload") or {}).get("shipment_id") or f"shipment-{index}")
    payload = dict(raw.get("payload") or {})
    if not payload and "eta_min" in raw:
        payload = {key: raw[key] for key in raw
                   if key not in {"record_id", "state", "version", "occurred_at", "source"}}
    if not payload:
        raise ContractError(f"事件 {shipment_id} 缺少 payload")
    state = normalized_text(raw.get("state") or (payload.get("journey_state") or "booked"))
    kind = OUTCOME if kind == OUTCOME or state in OUTCOME_STATES else OBSERVATION
    if kind == OUTCOME and state not in OUTCOME_STATES:
        state = normalized_text(payload.get("journey_state") or DELIVERED)
    revision = int(raw.get("revision") or raw.get("version") or 1)
    event_sequence = int(raw.get("event_sequence") or payload.get("event_sequence") or index)
    is_correction = bool(raw.get("is_correction")) or normalized_text(raw.get("correction")) == CORRECTION_KIND
    observed_at = str(raw.get("observed_at") or raw.get("occurred_at") or "")
    return StreamEvent(shipment_id=shipment_id, kind=kind,
                       occurred_at=str(raw.get("occurred_at") or payload.get("occurred_at") or ""),
                       revision=revision, payload=payload, state=state,
                       is_correction=is_correction, event_sequence=event_sequence,
                       record_id=str(raw.get("record_id") or ""),
                       source=str(raw.get("source") or "event-stream"),
                       observed_at=observed_at or str(raw.get("occurred_at") or ""))


def load_event_stream(value: Mapping[str, Any]) -> list[StreamEvent]:
    """加载并排序事件流，校验同一 (旅程, 观测点) 版本号单调递增。"""
    raw_events = value.get("events")
    if not isinstance(raw_events, list) or not raw_events:
        raise ContractError("事件流必须包含非空 events 列表")
    events = [_coerce_event(raw, index) for index, raw in enumerate(raw_events)]
    events.sort(key=lambda event: (event.time, event.event_sequence, event.shipment_id, event.revision))
    max_revision: dict[str, int] = {}
    for event in events:
        previous = max_revision.get(event.shipment_id, 0)
        if event.is_correction and event.revision <= previous:
            raise ContractError(
                f"旅程 {event.shipment_id} 的更正事件版本必须高于既有观测版本")
        max_revision[event.shipment_id] = max(previous, event.revision)
    return events


def latest_payload(versions: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = sorted(versions, key=lambda row: (int(row["revision"]), row["occurred_at"]))
    return dict(rows[-1]["payload"]) if rows else {}


@dataclass(frozen=True)
class HistoryRow:
    shipment_id: str
    route_code: str
    border: str
    cargo_class: str
    actual_minutes: float | None
    state: str
    features: dict[str, float]
    observed_at: str
    occurred_at: str
    revision: int
    is_exception: bool


@dataclass
class ReplayView:
    """某个决策时刻可见的世界快照。

    所有查询都以 cutoff（含）为界；cutoff 之后才到达的事件（含事后更正、
    事后修订的路况与容量）对该时点完全不可见。
    """

    cutoff: datetime
    observations: dict[str, list[StreamEvent]] = field(default_factory=dict)
    outcomes: dict[str, StreamEvent] = field(default_factory=dict)
    completed_shipments: set[str] = field(default_factory=set)
    open_shipments: set[str] = field(default_factory=set)
    stream_fingerprint: str = ""

    @classmethod
    def at(cls, events: Iterable[StreamEvent], cutoff: datetime) -> "ReplayView":
        visible = [event for event in events if event.time <= cutoff]
        view = cls(cutoff=cutoff)
        for event in visible:
            if event.kind == OBSERVATION:
                view.observations.setdefault(event.shipment_id, []).append(event)
                if event.shipment_id not in view.outcomes:
                    view.open_shipments.add(event.shipment_id)
            else:
                view.outcomes[event.shipment_id] = event
                view.completed_shipments.add(event.shipment_id)
                view.open_shipments.discard(event.shipment_id)
        view.open_shipments.difference_update(view.completed_shipments)
        view.stream_fingerprint = stable_hash([event.fingerprint for event in visible])
        return view

    def latest_observation(self, shipment_id: str) -> StreamEvent | None:
        rows = self.observations.get(shipment_id)
        if not rows:
            return None
        return sorted(rows, key=lambda event: (event.observed_time, event.revision, event.time))[-1]

    def observation_versions(self, shipment_id: str) -> list[StreamEvent]:
        return sorted(self.observations.get(shipment_id, []),
                      key=lambda event: (event.observed_time, event.revision, event.time))

    def outcome(self, shipment_id: str) -> StreamEvent | None:
        return self.outcomes.get(shipment_id)

    def history(self) -> list[HistoryRow]:
        """决策时点可见的全部已结束旅程，异常旅程同样保留。"""
        rows: list[HistoryRow] = []
        for shipment_id in sorted(self.completed_shipments):
            outcome = self.outcomes[shipment_id]
            observation = self.latest_observation(shipment_id)
            payload = observation.payload if observation else outcome.payload
            route_code = str(payload.get("route_code", ""))
            cargo = normalized_text(payload.get("cargo_class", "general"))
            actual = None
            if outcome.state == DELIVERED:
                actual = finite_number(outcome.payload.get("actual_eta_min", outcome.payload.get("eta_min")))
            features = {
                "border_queue": finite_number(payload.get("border_queue", 0.0)),
                "warehouse_load": finite_number(payload.get("warehouse_load", 0.0)),
                "temperature": finite_number(payload.get("temperature", 20.0)),
                "capacity_units": finite_number(payload.get("capacity_units", 0.0)),
                "eta_min": finite_number(payload.get("eta_min", 0.0)),
            }
            rows.append(HistoryRow(
                shipment_id=shipment_id, route_code=route_code,
                border=derive_border(route_code) if route_code else "unknown",
                cargo_class=cargo, actual_minutes=actual, state=outcome.state,
                features=features, observed_at=observation.observed_at if observation else outcome.observed_at,
                occurred_at=outcome.occurred_at, revision=outcome.revision,
                is_exception=outcome.is_exception))
        return rows

    def knowledge_fingerprint(self) -> str:
        """可见知识的稳定指纹：预测输入一旦改变，指纹随之改变。"""
        payload = {
            "cutoff": self.cutoff.isoformat(),
            "observations": [
                {"shipment_id": shipment_id,
                 "latest": self.latest_observation(shipment_id).fingerprint}
                for shipment_id in sorted(self.observations)],
            "outcomes": [event.fingerprint for _, event in sorted(self.outcomes.items())],
        }
        return stable_hash(payload)


def candidates_for(event: StreamEvent) -> tuple[str, ...]:
    """读取候选线路；缺省回退为事件自身 route_code 单候选。"""
    raw = event.payload.get("candidate_routes")
    if isinstance(raw, list) and raw:
        routes = tuple(str(item).strip() for item in raw if str(item).strip())
        if routes:
            return routes
    route = str(event.payload.get("route_code", "")).strip()
    if not route:
        raise ContractError(f"旅程 {event.shipment_id} 缺少 route_code，无法构造候选方案")
    return (route,)


def capacity_demand(event: StreamEvent) -> float:
    value = event.payload.get("capacity_demand", event.payload.get("capacity_units", 1.0))
    return max(0.0, finite_number(value))


def record_to_event(record: DomainRecord) -> StreamEvent:
    """把现有 DomainRecord 适配为事件流观测事件，便于复用既有夹具。"""
    return StreamEvent(shipment_id=record.record_id, kind=OBSERVATION,
                       occurred_at=record.occurred_at, revision=record.version,
                       payload=dict(record.payload), state=record.state,
                       event_sequence=int(record.payload.get("event_sequence", 0) or 0),
                       record_id=record.record_id, source=record.source)


def events_canonical_json(events: Iterable[StreamEvent]) -> str:
    return canonical_json([event.as_dict() for event in events])
