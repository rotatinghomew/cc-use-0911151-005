"""专项：决策时点信息纪律（point-in-time discipline）。

验证预测只能看到决策时刻已经到达的事件版本：
- 事后修订的路况（border_queue 更正）不得改变历史时点已生成的预测；
- 事后修订的容量只影响更正之后的推荐，不得回溯美化；
- 不可达/改道/冷链异常旅程不会从任何聚合口径中消失。
"""
from __future__ import annotations

import unittest

from corridor_twin.backtest.eta_forecast import EtaForecaster
from corridor_twin.backtest.event_stream import ReplayView
from corridor_twin.backtest.recommendation import (
    CAPACITY_INSUFFICIENT,
    EligibilityConfig,
    evaluate_eligibility,
)
from corridor_twin.backtest.rolling import RollingBacktest
from tests.backtest.test_event_stream import observation, outcome
from tests.backtest.test_rolling_backtest import history_stream


class PointInTimeDisciplineTests(unittest.TestCase):
    def test_late_road_correction_does_not_rewrite_earlier_forecast(self):
        events = history_stream([100.0, 110.0, 120.0])
        first = observation("open-1", "2026-11-03T00:00:00+00:00", sequence=90, queue=20)
        events.append(first)
        view_before = ReplayView.at(events, first.time)
        forecast_before = EtaForecaster().forecast(
            view_before, "open-1", first.payload["route_code"],
            first.occurred_at, live_event=first)

        # 更晚时刻才到达的路况更正（队列大幅上修）。
        correction = observation("open-1", "2026-11-03T09:00:00+00:00", sequence=91,
                                 revision=2, correction=True, queue=280)
        events.append(correction)
        view_after = ReplayView.at(events, first.time)  # 同样的历史 cutoff
        forecast_replayed = EtaForecaster().forecast(
            view_after, "open-1", first.payload["route_code"],
            first.occurred_at, live_event=first)

        # 在原决策 cutoff 重放，更正事件不可见 → 预测逐字段一致。
        self.assertEqual(forecast_before.as_dict(), forecast_replayed.as_dict())

        # 在更正 cutoff 重放才会看到新压力，且知识指纹不同。
        view_corrected = ReplayView.at(events, correction.time)
        forecast_corrected = EtaForecaster().forecast(
            view_corrected, "open-1", correction.payload["route_code"],
            correction.occurred_at, live_event=correction)
        self.assertGreater(forecast_corrected.pressure, forecast_before.pressure)
        self.assertNotEqual(forecast_corrected.knowledge_fingerprint,
                            forecast_before.knowledge_fingerprint)

    def test_capacity_uses_only_decision_time_visible_value(self):
        events = history_stream([100.0, 110.0, 120.0])
        events.append(observation("open-1", "2026-11-03T00:00:00+00:00", sequence=90))
        result = RollingBacktest(EtaForecaster(min_samples=2)).run(events)
        forecast = next(row.forecast for row in result.current_records()
                        if row.shipment_id == "open-1")
        key = f"{forecast.border}|{forecast.cargo_class}"
        metrics = result.segment_metrics()[key]
        config = EligibilityConfig(min_samples=2, max_mae_ratio=0.5,
                                   min_coverage=0.5, max_envelope_ratio=2.0)
        # 决策时点容量为 0：候选必须被容量门槛拦截，不能用未来扩容后的容量。
        blocked = evaluate_eligibility(forecast, metrics, config, demand=5.0, capacity_left=0.0)
        self.assertIn(CAPACITY_INSUFFICIENT, blocked.reasons)
        # 同一预测、同一证据，仅当决策时点可见容量充足时才合格。
        allowed = evaluate_eligibility(forecast, metrics, config, demand=5.0, capacity_left=10.0)
        self.assertTrue(allowed.eligible)

    def test_exception_denominator_never_drops_journeys(self):
        events = history_stream([100.0, 110.0])
        for sid, state in (("u1", "unreachable"), ("r1", "rerouted"),
                           ("c1", "cold_chain_exception"), ("d1", "delivered")):
            events.append(observation(sid, "2026-11-03T00:00:00+00:00", sequence=90))
            if state == "delivered":
                events.append(outcome(sid, "2026-11-03T02:00:00+00:00", actual=118.0, sequence=91))
            else:
                events.append(outcome(sid, "2026-11-03T02:00:00+00:00", state=state, sequence=91))
        result = RollingBacktest().run(events)
        overall = result.overall_metrics()
        # 全部 8 个旅程（2 历史 + 4 新旅程，每个 2 候选线路）都在预测分母里。
        shipment_ids = {row.shipment_id for row in result.records}
        self.assertEqual(shipment_ids, {"hist-00", "hist-01", "u1", "r1", "c1", "d1"})
        self.assertEqual(overall["exception_count"], 6)  # 3 异常旅程 × 2 候选
        self.assertEqual(overall["forecast_count"], 12)
        # 异常旅程不产生数值误差，但被显式标注，而非悄悄剔除。
        exception_scores = [row.score for row in result.records
                            if row.score and row.score.get("outcome_state") != "delivered"]
        self.assertTrue(all(score["scored"] is False for score in exception_scores))
        self.assertTrue(all(score["actual_minutes"] is None for score in exception_scores))


if __name__ == "__main__":
    unittest.main()
