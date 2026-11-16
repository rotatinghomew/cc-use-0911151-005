from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from corridor_twin.backtest.eta_forecast import EtaForecaster
from corridor_twin.backtest.recommendation import (
    CAPACITY_INSUFFICIENT,
    EligibilityConfig,
    INSUFFICIENT_SAMPLES,
    PRIOR_ONLY,
    Recommendation,
    evaluate_eligibility,
    recommend,
)
from corridor_twin.backtest.rolling import RollingBacktest
from corridor_twin.backtest.store import ACTIVE, ADOPTED, STALE, BacktestStore
from corridor_twin.events.logistics_event import ContractError
from corridor_twin.state.twin_repository import Repository
from tests.backtest.test_event_stream import observation
from tests.backtest.test_rolling_backtest import history_stream


class EligibilityTests(unittest.TestCase):
    def setUp(self):
        self.config = EligibilityConfig(min_samples=2, max_mae_ratio=0.5,
                                        min_coverage=0.5, max_envelope_ratio=2.0)
        events = history_stream([100.0, 110.0, 120.0])
        self.result = RollingBacktest(EtaForecaster(min_samples=2)).run(
            events + [observation("open-1", "2026-11-03T00:00:00+00:00", sequence=90)])
        self.forecast = next(row.forecast for row in self.result.current_records()
                             if row.shipment_id == "open-1")

    def test_eligible_candidate_carries_traceable_evidence(self):
        key = f"{self.forecast.border}|{self.forecast.cargo_class}"
        metrics = self.result.segment_metrics()[key]
        evaluation = evaluate_eligibility(self.forecast, metrics, self.config, 5.0, 10.0)
        self.assertTrue(evaluation.eligible)
        self.assertEqual(evaluation.reasons, ("eligible",))
        self.assertIn("forecast_id", evaluation.forecast)
        self.assertIn("sample_shipments", evaluation.forecast)
        self.assertEqual(evaluation.forecast["sample_size"], 3)

    def test_capacity_shortfall_blocks_candidate(self):
        evaluation = evaluate_eligibility(self.forecast, {"scored_count": 3, "mae": 5.0,
                                                          "coverage_rate": 1.0},
                                          self.config, demand=8.0, capacity_left=2.0)
        self.assertIn(CAPACITY_INSUFFICIENT, evaluation.reasons)
        self.assertFalse(evaluation.eligible)

    def test_prior_only_candidate_is_not_recommended(self):
        empty = RollingBacktest().run([observation("solo", "2026-11-01T00:00:00+00:00")])
        forecast = empty.current_records()[0].forecast
        evaluation = evaluate_eligibility(forecast, {}, self.config, 1.0, 10.0)
        self.assertIn(PRIOR_ONLY, evaluation.reasons)
        self.assertIn(INSUFFICIENT_SAMPLES, evaluation.reasons)

    def test_recommend_picks_fastest_eligible_route_and_explains_choice(self):
        capacities = {"nanning-pingxiang-hanoi": 20.0, "kunming-hanoi-vientiane": 20.0}
        event = next(event for event in self.result.events
                     if event.kind == "observation" and event.shipment_id == "open-1")
        recommendation = recommend(self.result, event, capacities, self.config)
        self.assertIsInstance(recommendation, Recommendation)
        self.assertIn(recommendation.selected_route, capacities)
        self.assertTrue(any("forecast_id" in reason for reason in recommendation.rationale))
        self.assertTrue(all(hasattr(item, "reasons") for item in recommendation.candidates))
        # 证据指纹稳定可复算。
        again = recommend(self.result, event, capacities, self.config)
        self.assertEqual(again.evidence_fingerprint, recommendation.evidence_fingerprint)

    def test_no_recommendation_when_all_candidates_lack_evidence(self):
        event = self.result.events[-1]
        strict = EligibilityConfig(min_samples=99)
        recommendation = recommend(self.result, event,
                                   {"nanning-pingxiang-hanoi": 20.0,
                                    "kunming-hanoi-vientiane": 20.0}, strict)
        self.assertIsNone(recommendation.selected_route)
        self.assertFalse(recommendation.has_recommendation)
        self.assertTrue(all(not item.eligible for item in recommendation.candidates))


class BacktestStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repository = Repository(Path(self.temp.name) / "bt.sqlite3")
        self.store = BacktestStore(self.repository)
        events = history_stream([100.0, 110.0, 120.0])
        self.result = RollingBacktest(EtaForecaster(min_samples=2)).run(
            events + [observation("open-1", "2026-11-03T00:00:00+00:00", sequence=90)])
        self.run_id = "run-test"
        self.store.save_run(self.run_id, "stream-fp", self.result.model_version, self.result)
        event = next(event for event in self.result.events
                     if event.kind == "observation" and event.shipment_id == "open-1")
        self.recommendation = recommend(
            self.result, event,
            {"nanning-pingxiang-hanoi": 20.0, "kunming-hanoi-vientiane": 20.0},
            EligibilityConfig(min_samples=2, max_mae_ratio=0.5,
                              min_coverage=0.5, max_envelope_ratio=2.0))

    def test_save_and_guard_active_recommendation(self):
        reco_id = self.store.save_recommendation(self.run_id, self.recommendation)
        row = self.store.guard(reco_id)
        self.assertEqual(row["status"], ACTIVE)

    def test_correction_marks_unissued_recommendation_stale_and_blocks_guard(self):
        reco_id = self.store.save_recommendation(self.run_id, self.recommendation)
        changed = self.store.mark_stale_for_correction(
            self.run_id, self.recommendation.shipment_id, "corrected-event-1")
        self.assertGreaterEqual(changed, 1)
        with self.assertRaisesRegex(ContractError, "过期"):
            self.store.guard(reco_id)
        with self.assertRaisesRegex(ContractError, "过期"):
            self.store.adopt(self.run_id, reco_id)

    def test_adopted_recommendation_is_frozen_against_later_correction(self):
        reco_id = self.store.save_recommendation(self.run_id, self.recommendation)
        self.store.adopt(self.run_id, reco_id)
        self.store.mark_stale_for_correction(
            self.run_id, self.recommendation.shipment_id, "corrected-event-2")
        # 已采纳建议不被更正影响，仍可查询且状态保持 adopted。
        self.assertEqual(self.store.guard(reco_id)["status"], ADOPTED)

    def test_reissuing_stale_recommendation_is_rejected(self):
        reco_id = self.store.save_recommendation(self.run_id, self.recommendation)
        self.store.mark_stale_for_correction(
            self.run_id, self.recommendation.shipment_id, "corrected-event-3")
        with self.assertRaisesRegex(ContractError, "禁止沿用过期结果"):
            self.store.save_recommendation(self.run_id, self.recommendation)

    def test_evidence_free_recommendation_cannot_be_issued(self):
        event = self.result.events[-1]
        empty = recommend(self.result, event, None, EligibilityConfig(min_samples=99))
        with self.assertRaisesRegex(ContractError, "证据不足"):
            self.store.save_recommendation(self.run_id, empty)

    def test_forecast_rows_include_superseded_status(self):
        self.store.mark_stale_for_correction(
            self.run_id, self.recommendation.shipment_id, "corrected-event-4")
        rows = self.store.forecast_rows(self.run_id, self.recommendation.shipment_id)
        statuses = {row["status"] for row in rows}
        self.assertIn("superseded", statuses)


if __name__ == "__main__":
    unittest.main()
