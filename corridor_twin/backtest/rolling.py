"""滚动回测引擎。

按 occurred_at 顺序重放事件流：
1. 每条观测事件到达时，以当时可见知识为每个候选线路生成 ETA 预测；
2. 观测事件若为事后更正，受影响且尚未发出（未采纳）的预测立即失效，
   并基于更正后的可见知识重算；
3. 旅程结束（含不可达、改道、冷链异常）时，对该旅程保留的全部预测版本
   计算事后误差；异常旅程始终计入口径，绝不静默剔除；
4. 已采纳的预测冻结不重写，仍允许事后比较其误差。
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from corridor_twin.backtest.eta_forecast import MODEL_VERSION, EtaForecaster, Forecast
from corridor_twin.backtest.event_stream import (
    COLD_CHAIN_EXCEPTION,
    EXCEPTION_STATES,
    OUTCOME,
    REROUTED,
    UNREACHABLE,
    StreamEvent,
    ReplayView,
    candidates_for,
    capacity_demand,
    load_event_stream,
)
from corridor_twin.events.logistics_event import finite_number, stable_hash

CURRENT = "current"
SUPERSEDED = "superseded"
ADOPTED = "adopted"
SCORED = "scored"


@dataclass
class ForecastRecord:
    forecast: Forecast
    shipment_id: str
    route_code: str
    status: str = CURRENT
    superseded_reason: str = ""
    superseded_by: str = ""
    corrected_event_id: str = ""
    score: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {"forecast": self.forecast.as_dict(), "shipment_id": self.shipment_id,
                "route_code": self.route_code, "status": self.status,
                "superseded_reason": self.superseded_reason, "superseded_by": self.superseded_by,
                "corrected_event_id": self.corrected_event_id, "score": self.score}


def _error_metrics(forecast: Forecast, actual: float) -> dict[str, Any]:
    error = forecast.point_minutes - actual
    absolute = abs(error)
    inside = forecast.p10_minutes <= actual <= forecast.p90_minutes
    return {
        "actual_minutes": round(actual, 4),
        "error_minutes": round(error, 4),
        "abs_error_minutes": round(absolute, 4),
        "ape": round(absolute / max(1.0, actual), 6),
        "coverage_hit": bool(inside),
        "envelope_width": round(forecast.envelope_width, 4),
        "scored": True,
    }


def _aggregate(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    numeric = [row for row in rows if row.get("scored")]
    result: dict[str, Any] = {
        "forecast_count": len(rows),
        "scored_count": len(numeric),
        "unreachable_count": sum(row.get("outcome_state") == UNREACHABLE for row in rows),
        "rerouted_count": sum(row.get("outcome_state") == REROUTED for row in rows),
        "cold_chain_exception_count": sum(row.get("outcome_state") == COLD_CHAIN_EXCEPTION for row in rows),
        "exception_count": sum(row.get("outcome_state") in EXCEPTION_STATES for row in rows),
    }
    if numeric:
        abs_errors = [row["abs_error_minutes"] for row in numeric]
        errors = [row["error_minutes"] for row in numeric]
        apes = [row["ape"] for row in numeric]
        hits = [row["coverage_hit"] for row in numeric]
        widths = [row["envelope_width"] for row in numeric]
        result.update({
            "mae": round(statistics.fmean(abs_errors), 4),
            "rmse": round(math.sqrt(statistics.fmean(value ** 2 for value in abs_errors)), 4),
            "bias": round(statistics.fmean(errors), 4),
            "mape": round(statistics.fmean(apes), 6),
            "coverage_rate": round(statistics.fmean(1.0 if hit else 0.0 for hit in hits), 4),
            "mean_envelope_width": round(statistics.fmean(widths), 4),
        })
    else:
        result.update({"mae": None, "rmse": None, "bias": None, "mape": None,
                       "coverage_rate": None, "mean_envelope_width": None})
    return result


@dataclass
class BacktestResult:
    records: list[ForecastRecord] = field(default_factory=list)
    events: list[StreamEvent] = field(default_factory=list)
    model_version: str = MODEL_VERSION

    def current_records(self) -> list[ForecastRecord]:
        return [row for row in self.records if row.status == CURRENT]

    def outcome_shipments(self) -> set[str]:
        return {row.shipment_id for row in self.records if row.score is not None}

    def records_for(self, shipment_id: str) -> list[ForecastRecord]:
        return [row for row in self.records if row.shipment_id == shipment_id]

    def scored_rows(self) -> list[dict[str, Any]]:
        rows = []
        for record in self.records:
            if record.score is None:
                continue
            row = dict(record.score)
            row.update({"shipment_id": record.shipment_id, "route_code": record.route_code,
                        "border": record.forecast.border, "cargo_class": record.forecast.cargo_class,
                        "evidence_level": record.forecast.evidence_level,
                        "forecast_status": record.status,
                        "outcome_state": record.score.get("outcome_state", "")})
            rows.append(row)
        return rows

    def segment_metrics(self) -> dict[str, dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in self.records:
            if record.score is None:
                continue
            key = f"{record.forecast.border}|{record.forecast.cargo_class}"
            payload = dict(record.score)
            payload["outcome_state"] = record.score.get("outcome_state", "")
            grouped.setdefault(key, []).append(payload)
        return {key: _aggregate(rows) for key, rows in sorted(grouped.items())}

    def overall_metrics(self) -> dict[str, Any]:
        payloads = []
        for record in self.records:
            if record.score is None:
                continue
            payload = dict(record.score)
            payload["outcome_state"] = record.score.get("outcome_state", "")
            payloads.append(payload)
        metrics = _aggregate(payloads)
        metrics["stale_superseded_count"] = sum(row.status == SUPERSEDED for row in self.records)
        metrics["adopted_count"] = sum(row.status == ADOPTED for row in self.records)
        metrics["open_forecast_count"] = sum(row.status == CURRENT for row in self.records)
        return metrics

    def as_dict(self) -> dict[str, Any]:
        return {"model_version": self.model_version,
                "overall": self.overall_metrics(),
                "segments": self.segment_metrics(),
                "forecasts": [row.as_dict() for row in self.records]}

    def stale_forecasts(self) -> list[dict[str, Any]]:
        return [row.as_dict() for row in self.records if row.status == SUPERSEDED]


class RollingBacktest:
    """事件驱动的滚动回测；全程不查看 cutoff 之后的任何信息。"""

    def __init__(self, forecaster: EtaForecaster | None = None,
                 adopted_shipments: Iterable[str] | None = None):
        self.forecaster = forecaster or EtaForecaster()
        # 已实际发出建议的旅程：其预测在整个重放中保持冻结，更正不重写。
        self.adopted_shipments = set(adopted_shipments or ())

    def run(self, events: Iterable[StreamEvent] | Mapping[str, Any],
            adopted_shipments: Iterable[str] | None = None) -> BacktestResult:
        if isinstance(events, Mapping):
            events = load_event_stream(events)
        events = sorted(events, key=lambda event: (event.time, event.event_sequence,
                                                    event.shipment_id, event.revision))
        result = BacktestResult(events=list(events))
        adopted = set(self.adopted_shipments if adopted_shipments is None else adopted_shipments)

        for event in events:
            view = ReplayView.at(events, event.time)
            if event.kind == OUTCOME:
                self._close_shipment(event, result, adopted)
                continue
            # 已采纳的建议冻结：更正既不使其失效，也不触发重算，
            # 但首个决策时点的预测仍正常生成（事件本身保留供事后审计）。
            if event.shipment_id in adopted:
                if not event.is_correction and not result.records_for(event.shipment_id):
                    self._issue_forecasts(event, view, result)
                continue
            # 观测事件：更正先使尚未发出的旧预测失效，再基于当前可见知识重算。
            if event.is_correction:
                self._invalidate(event, result)
            self._issue_forecasts(event, view, result)
        return result

    def _issue_forecasts(self, event: StreamEvent, view: ReplayView, result: BacktestResult) -> None:
        for route in candidates_for(event):
            forecast = self.forecaster.forecast(
                view, event.shipment_id, route, event.occurred_at, live_event=event)
            result.records.append(ForecastRecord(
                forecast=forecast, shipment_id=event.shipment_id, route_code=route))

    def _invalidate(self, event: StreamEvent, result: BacktestResult) -> None:
        for record in result.records:
            if record.shipment_id != event.shipment_id or record.status != CURRENT:
                continue
            record.status = SUPERSEDED
            record.superseded_reason = "observation_corrected"
            record.corrected_event_id = event.record_id

    def _close_shipment(self, event: StreamEvent, result: BacktestResult,
                        adopted: set[str]) -> None:
        rows = result.records_for(event.shipment_id)
        for record in rows:
            if record.score is not None:
                continue
            if event.state in EXCEPTION_STATES:
                record.score = {"scored": False, "outcome_state": event.state,
                                "actual_minutes": None, "error_minutes": None,
                                "abs_error_minutes": None, "ape": None,
                                "coverage_hit": None, "envelope_width": record.forecast.envelope_width}
                continue
            actual = finite_number(event.payload.get("actual_eta_min", event.payload.get("eta_min")))
            score = _error_metrics(record.forecast, actual)
            score["outcome_state"] = event.state
            record.score = score
        # 结局到达后，该旅程不再存在“未发出”的当前预测。
        for record in rows:
            if record.status == CURRENT:
                record.status = ADOPTED if record.shipment_id in adopted else SCORED
        adopted.discard(event.shipment_id)
    def mark_adopted(self, result: BacktestResult, shipment_id: str) -> list[str]:
        """建议实际发出后调用：冻结当前预测，之后更正不再重写它们。"""
        frozen = []
        for record in result.records:
            if record.shipment_id == shipment_id and record.status == CURRENT:
                record.status = ADOPTED
                frozen.append(record.forecast.forecast_id)
        return frozen


def backtest_fingerprint(result: BacktestResult) -> str:
    return stable_hash({"model": result.model_version,
                        "events": [event.fingerprint for event in result.events]})
