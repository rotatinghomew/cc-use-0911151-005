"""基于回测证据的线路推荐。

一条候选线路只有在其口岸×货类分段满足最小样本量、误差与覆盖率门槛、
且当前容量可满足需求时，才有资格被推荐。推荐结果携带完整证据
（样本旅程、误差指标、所用预测及知识指纹），可在实际路线建议中回溯；
证据不足的候选只会被标注原因，不会被静默丢弃。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from corridor_twin.backtest.eta_forecast import Forecast
from corridor_twin.backtest.event_stream import (
    StreamEvent,
    capacity_demand,
    derive_border,
)
from corridor_twin.backtest.rolling import BacktestResult
from corridor_twin.events.logistics_event import ContractError, stable_hash

# 证据门槛默认值；可通过构造参数覆盖。
DEFAULT_MIN_SAMPLES = 3
DEFAULT_MAX_MAE_RATIO = 0.30
DEFAULT_MIN_COVERAGE = 0.70
DEFAULT_MAX_ENVELOPE_RATIO = 0.75

INSUFFICIENT_SAMPLES = "insufficient_samples"
SEGMENT_NOT_EVALUATED = "segment_not_evaluated"
MAE_TOO_HIGH = "mae_above_threshold"
COVERAGE_TOO_LOW = "coverage_below_threshold"
ENVELOPE_TOO_WIDE = "uncertainty_too_wide"
CAPACITY_INSUFFICIENT = "capacity_insufficient"
PRIOR_ONLY = "prior_only_no_history"
ELIGIBLE = "eligible"


@dataclass(frozen=True)
class CandidateEvaluation:
    route_code: str
    border: str
    cargo_class: str
    eligible: bool
    reasons: tuple[str, ...]
    forecast: dict[str, Any]
    segment_metrics: dict[str, Any]
    demand: float
    capacity_left: float | None

    def as_dict(self) -> dict[str, Any]:
        return {"route_code": self.route_code, "border": self.border, "cargo_class": self.cargo_class,
                "eligible": self.eligible, "reasons": list(self.reasons),
                "forecast": self.forecast, "segment_metrics": self.segment_metrics,
                "demand": self.demand, "capacity_left": self.capacity_left}


@dataclass(frozen=True)
class Recommendation:
    shipment_id: str
    decided_at: str
    selected_route: str | None
    rationale: tuple[str, ...]
    candidates: tuple[CandidateEvaluation, ...]
    evidence_fingerprint: str
    thresholds: dict[str, Any]

    @property
    def has_recommendation(self) -> bool:
        return self.selected_route is not None

    def as_dict(self) -> dict[str, Any]:
        return {"shipment_id": self.shipment_id, "decided_at": self.decided_at,
                "selected_route": self.selected_route, "rationale": list(self.rationale),
                "candidates": [item.as_dict() for item in self.candidates],
                "evidence_fingerprint": self.evidence_fingerprint, "thresholds": dict(self.thresholds)}


@dataclass(frozen=True)
class EligibilityConfig:
    min_samples: int = DEFAULT_MIN_SAMPLES
    max_mae_ratio: float = DEFAULT_MAX_MAE_RATIO
    min_coverage: float = DEFAULT_MIN_COVERAGE
    max_envelope_ratio: float = DEFAULT_MAX_ENVELOPE_RATIO

    def as_dict(self) -> dict[str, Any]:
        return {"min_samples": self.min_samples, "max_mae_ratio": self.max_mae_ratio,
                "min_coverage": self.min_coverage, "max_envelope_ratio": self.max_envelope_ratio}


def evaluate_eligibility(forecast: Forecast, segment_metrics: Mapping[str, Any],
                         config: EligibilityConfig, demand: float,
                         capacity_left: float | None) -> CandidateEvaluation:
    reasons: list[str] = []
    samples = int(segment_metrics.get("scored_count", 0) or 0)
    if forecast.prior_used or samples == 0:
        reasons.append(PRIOR_ONLY)
    if samples < config.min_samples:
        reasons.append(INSUFFICIENT_SAMPLES)
    if not segment_metrics:
        reasons.append(SEGMENT_NOT_EVALUATED)
    mae = segment_metrics.get("mae")
    coverage = segment_metrics.get("coverage_rate")
    if mae is not None and forecast.point_minutes > 0:
        if mae / forecast.point_minutes > config.max_mae_ratio:
            reasons.append(MAE_TOO_HIGH)
    if coverage is not None and coverage < config.min_coverage:
        reasons.append(COVERAGE_TOO_LOW)
    if forecast.point_minutes > 0 and forecast.envelope_width / forecast.point_minutes > config.max_envelope_ratio:
        reasons.append(ENVELOPE_TOO_WIDE)
    if capacity_left is not None and capacity_left < demand:
        reasons.append(CAPACITY_INSUFFICIENT)
    eligible = not reasons
    return CandidateEvaluation(
        route_code=forecast.route_code, border=forecast.border, cargo_class=forecast.cargo_class,
        eligible=eligible, reasons=tuple(reasons) if reasons else (ELIGIBLE,),
        forecast=forecast.as_dict(), segment_metrics=dict(segment_metrics),
        demand=demand, capacity_left=capacity_left)


def _current_forecasts(result: BacktestResult, shipment_id: str) -> list[Forecast]:
    return [record.forecast for record in result.current_records() if record.shipment_id == shipment_id]


def recommend(result: BacktestResult, event: StreamEvent,
              capacities: Mapping[str, float] | None = None,
              config: EligibilityConfig | None = None) -> Recommendation:
    """根据回测证据给出推荐；容量以决策时点可见容量为准，禁止用未来容量。"""
    config = config or EligibilityConfig()
    capacities = dict(capacities or {})
    demand = capacity_demand(event)
    forecasts = _current_forecasts(result, event.shipment_id)
    if not forecasts:
        raise ContractError(f"旅程 {event.shipment_id} 没有当前有效预测，无法给出推荐")
    segment_metrics = result.segment_metrics()
    evaluations: list[CandidateEvaluation] = []
    for forecast in forecasts:
        key = f"{forecast.border}|{forecast.cargo_class}"
        capacity_left = capacities.get(forecast.route_code)
        evaluations.append(evaluate_eligibility(
            forecast, segment_metrics.get(key, {}), config, demand, capacity_left))

    eligible = [item for item in evaluations if item.eligible]
    selected = None
    rationale: list[str] = []
    if eligible:
        def ranking_key(item: CandidateEvaluation) -> tuple[float, float, str]:
            return (item.forecast["point_minutes"], item.forecast["envelope_width"], item.route_code)
        winner = min(eligible, key=ranking_key)
        selected = winner.route_code
        rationale = [
            f"point_minutes={winner.forecast['point_minutes']}",
            f"p10_p90={winner.forecast['p10_minutes']}..{winner.forecast['p90_minutes']}",
            f"segment_samples={winner.segment_metrics.get('scored_count', 0)}",
            f"mae={winner.segment_metrics.get('mae')}",
            f"coverage_rate={winner.segment_metrics.get('coverage_rate')}",
            f"forecast_id={winner.forecast['forecast_id']}",
            f"knowledge_fingerprint={winner.forecast['knowledge_fingerprint']}",
        ]
    else:
        rationale = ["no_candidate_meets_evidence_thresholds"]
    evidence_fingerprint = stable_hash({
        "shipment_id": event.shipment_id, "decided_at": event.occurred_at,
        "candidates": [item.as_dict() for item in evaluations],
        "thresholds": config.as_dict(), "demand": demand,
    })[:24]
    return Recommendation(
        shipment_id=event.shipment_id, decided_at=event.occurred_at,
        selected_route=selected, rationale=tuple(rationale),
        candidates=tuple(evaluations), evidence_fingerprint=evidence_fingerprint,
        thresholds=config.as_dict())
