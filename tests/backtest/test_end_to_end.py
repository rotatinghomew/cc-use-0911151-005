from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from corridor_twin.backtest.service import BacktestApplication
from corridor_twin.visibility.control_tower import (
    export_backtest_bundle,
    verify_backtest_bundle,
)
from corridor_twin.twin_service import load_fixture

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "backtest_events.json"


class BacktestEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / "backtest.sqlite3"
        self.output = self.root / "bundle"

    def test_fixture_backtest_retains_exceptions_and_splits_segments(self):
        application = BacktestApplication(self.database)
        report = application.run_backtest(load_fixture(FIXTURE))
        overall = report["overall"]
        # 不可达、改道、冷链异常各 4 条预测记录（每异常旅程 2 个候选）。
        self.assertEqual(overall["unreachable_count"], 4)
        self.assertEqual(overall["rerouted_count"], 4)
        self.assertEqual(overall["cold_chain_exception_count"], 4)
        self.assertEqual(overall["exception_count"], 12)
        # 异常旅程没有被静默剔除：分母包含全部已结束预测。
        self.assertGreaterEqual(overall["forecast_count"], overall["scored_count"])
        self.assertEqual(set(report["segments"]),
                         {"pingxiang|general", "pingxiang|cold", "hanoi|general", "hanoi|cold"})
        for metrics in report["segments"].values():
            self.assertIn("mae", metrics)
            self.assertIn("coverage_rate", metrics)

    def test_correction_produces_superseded_and_current_forecast_generations(self):
        application = BacktestApplication(self.database)
        report = application.run_backtest(load_fixture(FIXTURE))
        stale = report["stale_forecasts"]
        self.assertTrue(stale)
        self.assertTrue(all(row["superseded_reason"] == "observation_corrected" for row in stale))
        self.assertTrue(all(row["corrected_event_id"] for row in stale))
        # 更正后的旅程仍有当前有效的新一代预测。
        open_rows = report["overall"]["open_forecast_count"]
        self.assertGreater(open_rows, 0)

    def test_bundle_export_and_verification(self):
        application = BacktestApplication(self.database)
        report = application.run_backtest(load_fixture(FIXTURE))
        export_backtest_bundle(self.output, report)
        self.assertEqual(verify_backtest_bundle(self.output), [])
        recommendations = json.loads((self.output / "backtest-recommendations.json").read_text())
        self.assertTrue(all("evidence_fingerprint" in item for item in recommendations))
        # 篡改后必须被发现。
        segments_path = self.output / "backtest-segments.json"
        segments = json.loads(segments_path.read_text())
        segments["tampered"] = True
        segments_path.write_text(json.dumps(segments), encoding="utf-8")
        self.assertTrue(verify_backtest_bundle(self.output))

    def test_cli_backtest_and_verify(self):
        result = subprocess.run(
            [sys.executable, "-m", "corridor_twin", "backtest",
             "--fixture", str(FIXTURE), "--database", str(self.database),
             "--output", str(self.output)],
            cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, check=True)
        report = json.loads(result.stdout)
        self.assertIn("overall", report)
        verify = subprocess.run(
            [sys.executable, "-m", "corridor_twin", "backtest-verify",
             "--output", str(self.output)],
            cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, check=True)
        self.assertTrue(json.loads(verify.stdout)["ok"])

    def test_repeated_backtest_is_idempotent_and_correction_blocks_stale_issue(self):
        from corridor_twin.backtest.recommendation import EligibilityConfig, Recommendation
        from corridor_twin.events.logistics_event import ContractError

        application = BacktestApplication(self.database)
        config = EligibilityConfig(min_samples=3, max_envelope_ratio=1.0, min_coverage=0.6)
        capacities = {route: 20.0 for route in (
            "nanning-pingxiang-hanoi", "nanning-pingxiang-haiphong",
            "kunming-hanoi-vientiane", "nanning-hanoi-vientiane")}
        first = application.run_backtest(load_fixture(FIXTURE), capacities, config)
        second = application.run_backtest(load_fixture(FIXTURE), capacities, config)
        self.assertEqual(first["run_id"], second["run_id"])
        self.assertEqual(first["forecast_count"], second["forecast_count"])

        # J-open-02 的路况在事件流中被更正：其推荐一旦入库即应被 guard 阻断。
        reco_dict = next(item for item in second["recommendations"]
                         if item["shipment_id"] == "J-open-02")
        reco = Recommendation(
            shipment_id=reco_dict["shipment_id"], decided_at=reco_dict["decided_at"],
            selected_route=reco_dict["selected_route"], rationale=tuple(reco_dict["rationale"]),
            candidates=tuple(), evidence_fingerprint=reco_dict["evidence_fingerprint"],
            thresholds=reco_dict["thresholds"])
        reco_id = application.issue(second["run_id"], reco)
        application.store.mark_stale_for_correction(
            second["run_id"], "J-open-02", "correction-event")
        with self.assertRaisesRegex(ContractError, "过期"):
            application.guard(reco_id)


if __name__ == "__main__":
    unittest.main()
