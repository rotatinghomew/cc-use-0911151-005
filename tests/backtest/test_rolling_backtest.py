from __future__ import annotations

import unittest

from corridor_twin.backtest.eta_forecast import EtaForecaster, pressure_from_features
from corridor_twin.backtest.event_stream import ReplayView
from corridor_twin.backtest.rolling import (
    ADOPTED,
    CURRENT,
    SUPERSEDED,
    RollingBacktest,
)
from tests.backtest.test_event_stream import observation, outcome


def history_stream(samples, route="nanning-pingxiang-hanoi", cargo="general"):
    events = []
    for index, actual in enumerate(samples):
        sid = f"hist-{index:02d}"
        events.append(observation(sid, f"2026-11-01T{index:02d}:00:00+00:00",
                                  route=route, cargo=cargo, sequence=index))
        events.append(outcome(sid, f"2026-11-02T{index:02d}:00:00+00:00",
                              actual=float(actual), sequence=index + 100))
    return events


class ForecasterTests(unittest.TestCase):
    def test_uncertainty_envelope_brackets_point_and_widens_without_history(self):
        events = history_stream([100.0, 110.0, 120.0])
        view = ReplayView.at(events, events[-1].time)
        decision = observation("open-1", "2026-11-03T00:00:00+00:00")
        forecast = EtaForecaster().forecast(view, "open-1", decision.payload["route_code"],
                                            decision.occurred_at, live_event=decision)
        self.assertLessEqual(forecast.p10_minutes, forecast.point_minutes)
        self.assertGreaterEqual(forecast.p90_minutes, forecast.point_minutes)
        self.assertEqual(forecast.sample_size, 3)
        self.assertFalse(forecast.prior_used)
        self.assertEqual(forecast.evidence_level, "segment")
        self.assertEqual(len(forecast.sample_shipments), 3)

    def test_prior_forecast_when_no_matching_history(self):
        view = ReplayView.at([], parse_time("2026-11-01T00:00:00+00:00"))
        decision = observation("open-1", "2026-11-01T00:00:00+00:00", cargo="medical")
        forecast = EtaForecaster(min_samples=3).forecast(
            view, "open-1", decision.payload["route_code"], decision.occurred_at,
            live_event=decision)
        self.assertTrue(forecast.prior_used)
        self.assertEqual(forecast.sample_size, 0)
        self.assertEqual(forecast.evidence_level, "global_prior")
        self.assertGreater(forecast.envelope_width, 0.0)

    def test_cold_chain_pressure_rises_with_temperature_deviation(self):
        normal = pressure_from_features({"border_queue": 0, "warehouse_load": 0,
                                         "temperature": 4.0}, "cold")
        broken = pressure_from_features({"border_queue": 0, "warehouse_load": 0,
                                         "temperature": 20.0}, "cold")
        self.assertGreater(broken, normal)
        # 非冷链货类的冷链温度风险项恒为 0（总压力可由其他因子贡献）。
        from corridor_twin.backtest.eta_forecast import cold_pressure
        self.assertEqual(cold_pressure("general", 30.0), 0.0)
        self.assertGreater(cold_pressure("cold", 30.0), 0.0)

    def test_border_fallback_uses_same_border_other_cargo(self):
        events = history_stream([100.0, 110.0, 120.0], cargo="general")
        view = ReplayView.at(events, events[-1].time)
        decision = observation("open-1", "2026-11-03T00:00:00+00:00", cargo="cold")
        forecast = EtaForecaster().forecast(view, "open-1", decision.payload["route_code"],
                                            decision.occurred_at, live_event=decision)
        self.assertEqual(forecast.evidence_level, "border_fallback")
        self.assertEqual(forecast.cargo_class, "cold")

    def test_forecast_is_deterministic_and_fingerprinted(self):
        events = history_stream([100.0, 110.0, 120.0])
        view = ReplayView.at(events, events[-1].time)
        decision = observation("open-1", "2026-11-03T00:00:00+00:00")
        first = EtaForecaster().forecast(view, "open-1", decision.payload["route_code"],
                                         decision.occurred_at, live_event=decision)
        second = EtaForecaster().forecast(view, "open-1", decision.payload["route_code"],
                                          decision.occurred_at, live_event=decision)
        self.assertEqual(first.forecast_id, second.forecast_id)
        self.assertEqual(first.as_dict(), second.as_dict())


def parse_time(text):
    from corridor_twin.backtest.event_stream import parse_event_time
    return parse_event_time(text)


class RollingBacktestTests(unittest.TestCase):
    def setUp(self):
        self.engine = RollingBacktest(EtaForecaster(min_samples=2))

    def test_scores_forecasts_against_later_actuals_without_leakage(self):
        events = history_stream([100.0, 120.0, 140.0])
        events.append(observation("open-1", "2026-11-03T00:00:00+00:00", sequence=90))
        events.append(outcome("open-1", "2026-11-03T05:00:00+00:00", actual=130.0, sequence=91))
        result = self.engine.run(events)
        scored = [row for row in result.records if row.score and row.score.get("scored")]
        self.assertTrue(scored)
        for row in scored:
            self.assertIn("abs_error_minutes", row.score)
            self.assertIn("coverage_hit", row.score)
            self.assertIsInstance(row.score["coverage_hit"], bool)
        # open-1 的预测只允许看到其决策时点之前完成的 3 条历史。
        open_rows = [row for row in result.records if row.shipment_id == "open-1"]
        self.assertTrue(all(set(row.forecast.sample_shipments) <=
                            {"hist-00", "hist-01", "hist-02"} for row in open_rows))

    def test_exception_journeys_remain_in_denominator(self):
        events = history_stream([100.0, 110.0])
        events.append(observation("lost-1", "2026-11-03T00:00:00+00:00", sequence=90))
        events.append(outcome("lost-1", "2026-11-03T02:00:00+00:00",
                              state="unreachable", sequence=91))
        result = self.engine.run(events)
        lost = [row for row in result.records if row.shipment_id == "lost-1"]
        self.assertTrue(lost)
        self.assertTrue(all(row.score["outcome_state"] == "unreachable" for row in lost))
        self.assertTrue(all(row.score["scored"] is False for row in lost))
        overall = result.overall_metrics()
        self.assertEqual(overall["unreachable_count"], len(lost))
        self.assertEqual(overall["forecast_count"], len(result.records))

    def test_reroute_and_cold_chain_exceptions_are_counted_separately(self):
        events = history_stream([100.0, 110.0])
        for sid, state in (("rr", "rerouted"), ("cc", "cold_chain_exception")):
            events.append(observation(sid, "2026-11-03T00:00:00+00:00", sequence=90))
            events.append(outcome(sid, "2026-11-03T02:00:00+00:00", state=state, sequence=91))
        result = self.engine.run(events)
        overall = result.overall_metrics()
        self.assertEqual(overall["rerouted_count"], 2)
        self.assertEqual(overall["cold_chain_exception_count"], 2)

    def test_correction_invalidates_pending_forecasts_and_issues_recomputed_ones(self):
        events = history_stream([100.0, 110.0, 120.0])
        events.append(observation("open-1", "2026-11-03T00:00:00+00:00", sequence=90, queue=20))
        events.append(observation("open-1", "2026-11-03T06:00:00+00:00", sequence=91,
                                  revision=2, correction=True, queue=260))
        result = self.engine.run(events)
        rows = result.records_for("open-1")
        self.assertEqual([row.status for row in rows].count(SUPERSEDED), 2)
        self.assertEqual([row.status for row in rows].count(CURRENT), 2)
        stale = result.stale_forecasts()
        self.assertEqual(len(stale), 2)
        self.assertTrue(all(row["corrected_event_id"] for row in stale))
        # 重算预测使用了更正后的知识，指纹与被失效预测不同。
        superseded_ids = {row.forecast.knowledge_fingerprint for row in rows if row.status == SUPERSEDED}
        current_ids = {row.forecast.knowledge_fingerprint for row in rows if row.status == CURRENT}
        self.assertTrue(superseded_ids.isdisjoint(current_ids))

    def test_adopted_forecasts_are_frozen_and_still_scored(self):
        events = history_stream([100.0, 110.0, 120.0])
        events.append(observation("open-1", "2026-11-03T00:00:00+00:00", sequence=90))
        events.append(observation("open-1", "2026-11-03T06:00:00+00:00", sequence=91,
                                  revision=2, correction=True, queue=260))
        events.append(outcome("open-1", "2026-11-03T08:00:00+00:00", actual=128.0, sequence=92))
        # 未采纳：更正会失效并重算，产生两代预测。
        unadopted = self.engine.run(events)
        self.assertTrue(any(row.status == SUPERSEDED for row in unadopted.records_for("open-1")))
        # 已采纳：同一事件流下更正既不失效也不重算，预测只生成一次且冻结、可评分。
        result = self.engine.run(events, adopted_shipments={"open-1"})
        rows = result.records_for("open-1")
        self.assertEqual(len(rows), 2)  # 两个候选线路，各仅一条预测
        self.assertTrue(all(row.status == ADOPTED for row in rows))
        self.assertFalse(any(row.status == SUPERSEDED for row in rows))
        self.assertTrue(all(row.score is not None and row.score.get("scored") for row in rows))
        self.assertTrue(all(row.score["outcome_state"] == "delivered" for row in rows))
        # 冻结预测依据的是更正前的知识（低排队压力），未被事后修订污染。
        self.assertTrue(all(row.forecast.pressure < 0.5 for row in rows))

    def test_segment_metrics_split_by_border_and_cargo(self):
        events = history_stream([100.0, 110.0], route="nanning-pingxiang-hanoi", cargo="general")
        events += history_stream([200.0, 220.0], route="kunming-hanoi-vientiane", cargo="cold")
        result = self.engine.run(events)
        segments = result.segment_metrics()
        # 精确分段必然存在；回退分段（同口岸跨货类）允许同时出现。
        self.assertIn("pingxiang|general", segments)
        self.assertIn("hanoi|cold", segments)
        for metrics in segments.values():
            self.assertIsNotNone(metrics["mae"])
            self.assertIn("coverage_rate", metrics)


if __name__ == "__main__":
    unittest.main()
