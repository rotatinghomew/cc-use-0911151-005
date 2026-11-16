"""到达时间（ETA）滚动预测模型。

模型只用决策时点可见、且已经结束的旅程作为训练样本；异常旅程不进入
数值样本但会在证据中显式计数，保证调用方知道分母里有多少异常。
预测输出点估计与 P10/P90 不确定性包络，并携带样本清单与知识指纹，
使每一条预测都可以在实际路线建议中回溯。
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from corridor_twin.backtest.event_stream import (
    HistoryRow,
    ReplayView,
    StreamEvent,
    derive_border,
    route_parts,
)
from corridor_twin.events.logistics_event import bounded, canonical_json, finite_number, normalized_text, stable_hash

MODEL_VERSION = "eta-rolling-v1"
# 正态分布 P10/P90 分位数。
Z_80 = 1.2815515655446004
MIN_ETA_MINUTES = 1.0
DEFAULT_PRIOR_SIGMA_RATIO = 0.35
COLD_CARGO = {"cold", "fresh", "medical"}


@dataclass(frozen=True)
class Forecast:
    forecast_id: str
    shipment_id: str
    route_code: str
    border: str
    cargo_class: str
    decided_at: str
    point_minutes: float
    p10_minutes: float
    p90_minutes: float
    sigma_minutes: float
    sample_size: int
    exception_count: int
    evidence_level: str
    sample_shipments: tuple[str, ...]
    prior_used: bool
    knowledge_fingerprint: str
    model_version: str = MODEL_VERSION
    pressure: float = 0.0
    feature_basis: str = "history"
    extra: Mapping[str, Any] = field(default_factory=dict)

    @property
    def envelope_width(self) -> float:
        return self.p90_minutes - self.p10_minutes

    def as_dict(self) -> dict[str, Any]:
        return {
            "forecast_id": self.forecast_id, "shipment_id": self.shipment_id,
            "route_code": self.route_code, "border": self.border, "cargo_class": self.cargo_class,
            "decided_at": self.decided_at, "point_minutes": self.point_minutes,
            "p10_minutes": self.p10_minutes, "p90_minutes": self.p90_minutes,
            "envelope_width": round(self.envelope_width, 4),
            "sigma_minutes": self.sigma_minutes, "sample_size": self.sample_size,
            "exception_count": self.exception_count, "evidence_level": self.evidence_level,
            "sample_shipments": list(self.sample_shipments), "prior_used": self.prior_used,
            "knowledge_fingerprint": self.knowledge_fingerprint, "model_version": self.model_version,
            "pressure": self.pressure, "feature_basis": self.feature_basis, "extra": dict(self.extra),
        }


def cold_pressure(cargo_class: str, temperature: float) -> float:
    if normalized_text(cargo_class) not in COLD_CARGO:
        return 0.0
    return bounded(abs(temperature - 4.0) / 30.0)


def customs_pressure(customs_status: Any) -> float:
    return 0.8 if normalized_text(customs_status) not in {"clear", "released", "approved", ""} else 0.1


def pressure_from_features(features: Mapping[str, float], cargo_class: str, customs_status: Any = "") -> float:
    border = bounded(finite_number(features.get("border_queue", 0.0)) / 300.0)
    warehouse = bounded(finite_number(features.get("warehouse_load", 0.0)))
    cold = cold_pressure(cargo_class, finite_number(features.get("temperature", 20.0)))
    customs = customs_pressure(customs_status)
    return bounded(border * 0.30 + warehouse * 0.25 + customs * 0.25 + cold * 0.20)


def segment_key(border: str, cargo_class: str) -> str:
    return f"{normalized_text(border)}|{normalized_text(cargo_class)}"


class EtaForecaster:
    """基于口岸×货类历史样本的滚动预测器；无样本时退回到保守先验。"""

    def __init__(self, min_samples: int = 3, prior_sigma_ratio: float = DEFAULT_PRIOR_SIGMA_RATIO):
        self.min_samples = min_samples
        self.prior_sigma_ratio = prior_sigma_ratio

    def _segment_rows(self, history: Sequence[HistoryRow], border: str, cargo: str) -> list[HistoryRow]:
        cargo = normalized_text(cargo)
        exact = [row for row in history if row.border == border and row.cargo_class == cargo]
        if exact:
            return exact
        border_only = [row for row in history if row.border == border]
        if border_only:
            return border_only
        return list(history)

    def _evidence_level(self, history: Sequence[HistoryRow], border: str, cargo: str,
                        exact_count: int) -> tuple[str, int]:
        if exact_count >= self.min_samples:
            return "segment", exact_count
        if any(row.border == border for row in history):
            return "border_fallback", exact_count
        return "global_prior", exact_count

    def _mean_features(self, rows: Sequence[HistoryRow]) -> dict[str, float]:
        keys = ("border_queue", "warehouse_load", "temperature", "capacity_units", "eta_min")
        return {key: statistics.fmean(row.features[key] for row in rows) for key in keys}

    def forecast(self, view: ReplayView, shipment_id: str, route_code: str,
                 decided_at: str, live_event: StreamEvent | None = None) -> Forecast:
        history = view.history()
        border = derive_border(route_code)
        cargo = normalized_text(live_event.payload.get("cargo_class", "general")) if live_event else "general"
        planned_route = str(live_event.payload.get("route_code", "")) if live_event else ""
        is_planned = bool(live_event and normalized_text(planned_route) == normalized_text(route_code))

        rows = self._segment_rows(history, border, cargo)
        delivered = [row for row in rows if row.actual_minutes is not None]
        exception_count = sum(1 for row in rows if row.is_exception)
        exact_count = sum(1 for row in history
                          if row.border == border and row.cargo_class == cargo and row.actual_minutes is not None)
        evidence_level, _ = self._evidence_level(history, border, cargo, exact_count)

        if is_planned:
            features = {
                "border_queue": finite_number(live_event.payload.get("border_queue", 0.0)),
                "warehouse_load": finite_number(live_event.payload.get("warehouse_load", 0.0)),
                "temperature": finite_number(live_event.payload.get("temperature", 20.0)),
                "capacity_units": finite_number(live_event.payload.get("capacity_units", 0.0)),
                "eta_min": finite_number(live_event.payload.get("eta_min", 0.0)),
            }
            feature_basis = "live_observation"
            customs_status = live_event.payload.get("customs_status", "")
        else:
            features = self._mean_features(delivered or rows) if (delivered or rows) else {
                "border_queue": 0.0, "warehouse_load": 0.0, "temperature": 20.0,
                "capacity_units": 0.0, "eta_min": finite_number(live_event.payload.get("eta_min", 60.0))
                if live_event else 60.0}
            feature_basis = "segment_history"
            customs_status = ""
        pressure = pressure_from_features(features, cargo, customs_status)

        prior_used = not delivered
        if delivered:
            actuals = [row.actual_minutes for row in delivered]
            base_eta = statistics.fmean(actuals)
            point = base_eta * (1.0 + 0.15 * pressure)
            spread = statistics.pstdev(actuals) if len(actuals) > 1 else 0.0
            residual = statistics.fmean(abs(row.features["eta_min"] - row.actual_minutes) for row in delivered)
            # 小样本时向先验区间收缩，避免极少样本给出虚假的高置信度。
            shrink = max(1.0, self.min_samples / max(1, len(delivered)))
            sigma = max(spread, residual, base_eta * self.prior_sigma_ratio / shrink)
            sample_shipments = tuple(row.shipment_id for row in delivered)
        else:
            planned_eta = max(MIN_ETA_MINUTES, finite_number(features.get("eta_min", 60.0)))
            point = planned_eta * (1.0 + 0.15 * pressure)
            sigma = planned_eta * self.prior_sigma_ratio
            sample_shipments = ()

        point = max(MIN_ETA_MINUTES, point)
        p10 = max(MIN_ETA_MINUTES, point - Z_80 * sigma)
        p90 = max(point + Z_80 * sigma, p10 + MIN_ETA_MINUTES)
        knowledge = view.knowledge_fingerprint()
        forecast_id = stable_hash({
            "shipment_id": shipment_id, "route_code": route_code, "decided_at": decided_at,
            "model": MODEL_VERSION, "knowledge": knowledge,
            "features": features, "pressure": pressure,
        })[:24]
        return Forecast(
            forecast_id=forecast_id, shipment_id=shipment_id, route_code=route_code,
            border=border, cargo_class=cargo, decided_at=decided_at,
            point_minutes=round(point, 4), p10_minutes=round(p10, 4), p90_minutes=round(p90, 4),
            sigma_minutes=round(sigma, 4), sample_size=len(delivered),
            exception_count=exception_count, evidence_level=evidence_level,
            sample_shipments=sample_shipments, prior_used=prior_used,
            knowledge_fingerprint=knowledge, pressure=round(pressure, 6),
            feature_basis=feature_basis,
            extra={"exact_segment_samples": exact_count,
                   "route_nodes": list(route_parts(route_code))})


def forecast_canonical(forecast: Forecast) -> str:
    return canonical_json(forecast.as_dict())
