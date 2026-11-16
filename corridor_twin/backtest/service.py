"""滚动回测应用服务：编排事件重放、回测、推荐与持久化。"""
from __future__ import annotations

from pathlib import Path
from typing import Mapping

from corridor_twin.backtest.event_stream import StreamEvent, load_event_stream
from corridor_twin.backtest.eta_forecast import EtaForecaster
from corridor_twin.backtest.recommendation import EligibilityConfig, Recommendation, recommend
from corridor_twin.backtest.rolling import BacktestResult, RollingBacktest
from corridor_twin.backtest.store import BacktestStore
from corridor_twin.events.logistics_event import ContractError, stable_hash
from corridor_twin.state.twin_repository import Repository
from corridor_twin.twin_service import load_fixture


class BacktestApplication:
    def __init__(self, database: str | Path, min_samples: int = 3):
        self.repository = Repository(database)
        self.store = BacktestStore(self.repository)
        self.forecaster = EtaForecaster(min_samples=min_samples)
        self.engine = RollingBacktest(self.forecaster)

    def run_backtest(self, fixture: Mapping[str, Any] | str | Path,
                     capacities: Mapping[str, float] | None = None,
                     config: EligibilityConfig | None = None) -> dict[str, Any]:
        if isinstance(fixture, (str, Path)):
            fixture = load_fixture(fixture)
        events = load_event_stream(fixture)
        result = self.engine.run(events)
        stream_fp = stable_hash([event.fingerprint for event in events])
        run_id = stable_hash({"stream": stream_fp, "model": result.model_version})[:24]
        self.store.save_run(run_id, stream_fp, result.model_version, result)

        # 更正事件到达后，持久层中尚未发出的推荐同步失效。
        for event in events:
            if event.is_correction:
                self.store.mark_stale_for_correction(run_id, event.shipment_id, event.record_id)

        recommendations = self._build_recommendations(result, events, capacities, config)
        return {"run_id": run_id, "stream_fingerprint": stream_fp,
                "model_version": result.model_version,
                "overall": result.overall_metrics(),
                "segments": result.segment_metrics(),
                "recommendations": [item.as_dict() for item in recommendations],
                "stale_forecasts": result.stale_forecasts(),
                "forecast_count": len(result.records)}

    def _build_recommendations(self, result: BacktestResult, events: list[StreamEvent],
                               capacities: Mapping[str, float] | None,
                               config: EligibilityConfig | None) -> list[Recommendation]:
        latest: dict[str, StreamEvent] = {}
        for event in events:
            if event.kind == "observation" and event.shipment_id not in result.outcome_shipments():
                latest[event.shipment_id] = event
        recommendations = []
        for shipment_id in sorted(latest):
            if not any(record.shipment_id == shipment_id and record.status == "current"
                       for record in result.records):
                continue
            try:
                recommendations.append(recommend(result, latest[shipment_id], capacities, config))
            except ContractError:
                continue
        return recommendations

    def issue(self, run_id: str, recommendation: Recommendation) -> str:
        return self.store.save_recommendation(run_id, recommendation)

    def adopt(self, run_id: str, recommendation_id: str) -> None:
        self.store.adopt(run_id, recommendation_id)

    def guard(self, recommendation_id: str) -> dict[str, Any]:
        return self.store.guard(recommendation_id)
