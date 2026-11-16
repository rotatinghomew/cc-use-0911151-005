"""滚动回测的持久化与过期结果阻断。

预测与推荐写入 SQLite，并在事件更正后把受影响、尚未发出的推荐标记为
stale。任何调用方在实际发出路线建议前都必须通过 guard 检查；已发出
（adopted）的建议冻结保留，其预测不被重写，只用于事后误差比较。
"""
from __future__ import annotations

import json
from typing import Any

from corridor_twin.backtest.recommendation import Recommendation
from corridor_twin.backtest.rolling import BacktestResult, CURRENT, SUPERSEDED
from corridor_twin.events.logistics_event import ContractError, canonical_json, utc_now

STALE = "stale"
ACTIVE = "active"
ADOPTED = "adopted"

SCHEMA = """
CREATE TABLE IF NOT EXISTS backtest_runs(
    run_id TEXT PRIMARY KEY,
    stream_fingerprint TEXT NOT NULL,
    model_version TEXT NOT NULL,
    status TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backtest_forecasts(
    forecast_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    route_code TEXT NOT NULL,
    status TEXT NOT NULL,
    forecast_json TEXT NOT NULL,
    score_json TEXT,
    knowledge_fingerprint TEXT NOT NULL,
    corrected_event_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    PRIMARY KEY(run_id, forecast_id)
);
CREATE TABLE IF NOT EXISTS route_recommendations(
    recommendation_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    status TEXT NOT NULL,
    selected_route TEXT,
    recommendation_json TEXT NOT NULL,
    stale_reason TEXT NOT NULL DEFAULT '',
    corrected_event_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bt_forecast_shipment ON backtest_forecasts(shipment_id, status);
CREATE INDEX IF NOT EXISTS idx_reco_shipment ON route_recommendations(shipment_id, status);
"""


class BacktestStore:
    def __init__(self, repository):
        self.repository = repository
        with repository.connect() as connection:
            connection.executescript(SCHEMA)

    # ---- 回测批次 ----
    def save_run(self, run_id: str, stream_fingerprint: str, model_version: str,
                 result: BacktestResult) -> None:
        with self.repository.transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO backtest_runs VALUES(?,?,?,?,?,?)",
                (run_id, stream_fingerprint, model_version, "finished",
                 canonical_json(result.as_dict()), utc_now()))
            for record in result.records:
                forecast = record.forecast
                connection.execute(
                    "INSERT OR IGNORE INTO backtest_forecasts VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (forecast.forecast_id, run_id, record.shipment_id, record.route_code,
                     record.status, canonical_json(forecast.as_dict()),
                     canonical_json(record.score) if record.score is not None else None,
                     forecast.knowledge_fingerprint, record.corrected_event_id, utc_now()))

    def list_runs(self) -> list[dict[str, Any]]:
        with self.repository.connect() as connection:
            rows = connection.execute("SELECT * FROM backtest_runs ORDER BY created_at").fetchall()
        return [dict(row) for row in rows]

    # ---- 推荐生命周期 ----
    def save_recommendation(self, run_id: str, recommendation: Recommendation) -> str:
        if not recommendation.has_recommendation:
            raise ContractError("证据不足的候选不能作为路线建议发出")
        with self.repository.transaction() as connection:
            self._require_run(connection, run_id)
            existing = connection.execute(
                "SELECT recommendation_id,status FROM route_recommendations WHERE recommendation_id=?",
                (recommendation.evidence_fingerprint,)).fetchone()
            if existing:
                if existing["status"] == STALE:
                    raise ContractError("该建议所依据的事件已被更正，禁止沿用过期结果")
                return existing["recommendation_id"]
            connection.execute(
                "INSERT INTO route_recommendations VALUES(?,?,?,?,?,?,?,?,?,?)",
                (recommendation.evidence_fingerprint, run_id, recommendation.shipment_id,
                 recommendation.decided_at, ACTIVE, recommendation.selected_route,
                 canonical_json(recommendation.as_dict()), "", "", utc_now()))
        return recommendation.evidence_fingerprint

    def mark_stale_for_correction(self, run_id: str, shipment_id: str,
                                  corrected_event_id: str) -> int:
        """更正事件到达：未发出的活跃推荐全部失效；已采纳的保持冻结。"""
        changed = 0
        with self.repository.transaction() as connection:
            cursor = connection.execute(
                "UPDATE route_recommendations SET status=?, stale_reason=?, corrected_event_id=? "
                "WHERE run_id=? AND shipment_id=? AND status=?",
                (STALE, "observation_corrected", corrected_event_id,
                 run_id, shipment_id, ACTIVE))
            changed += cursor.rowcount
            connection.execute(
                "UPDATE backtest_forecasts SET status=?, corrected_event_id=? "
                "WHERE run_id=? AND shipment_id=? AND status=?",
                (SUPERSEDED, corrected_event_id, run_id, shipment_id, CURRENT))
        return changed

    def adopt(self, run_id: str, recommendation_id: str) -> None:
        """实际发出建议：冻结。冻结后任何更正都不能改写或阻断它。"""
        with self.repository.transaction() as connection:
            self._require_run(connection, run_id)
            row = connection.execute(
                "SELECT status FROM route_recommendations WHERE recommendation_id=?",
                (recommendation_id,)).fetchone()
            if row is None:
                raise ContractError("路线建议不存在")
            if row["status"] == STALE:
                raise ContractError("不能采纳已被更正事件失效的过期建议")
            connection.execute(
                "UPDATE route_recommendations SET status=? WHERE recommendation_id=?",
                (ADOPTED, recommendation_id))
            recommendation = json.loads(connection.execute(
                "SELECT recommendation_json FROM route_recommendations WHERE recommendation_id=?",
                (recommendation_id,)).fetchone()["recommendation_json"])
            shipment_id = recommendation["shipment_id"]
            selected = recommendation["selected_route"]
            connection.execute(
                "UPDATE backtest_forecasts SET status=? WHERE run_id=? AND shipment_id=? AND route_code=? AND status=?",
                (ADOPTED, run_id, shipment_id, selected, CURRENT))

    def guard(self, recommendation_id: str) -> dict[str, Any]:
        """发建议前的强制校验：过期建议直接拒绝。"""
        with self.repository.connect() as connection:
            row = connection.execute(
                "SELECT * FROM route_recommendations WHERE recommendation_id=?",
                (recommendation_id,)).fetchone()
        if row is None:
            raise ContractError("路线建议不存在")
        if row["status"] == STALE:
            raise ContractError(
                f"路线建议已过期（事件 {row['corrected_event_id']} 被更正），请基于最新回测重新生成")
        return dict(row)

    def recommendations_for(self, shipment_id: str) -> list[dict[str, Any]]:
        with self.repository.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM route_recommendations WHERE shipment_id=? ORDER BY created_at",
                (shipment_id,)).fetchall()
        return [dict(row) for row in rows]

    def forecast_rows(self, run_id: str, shipment_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM backtest_forecasts WHERE run_id=?"
        params: list[Any] = [run_id]
        if shipment_id:
            query += " AND shipment_id=?"
            params.append(shipment_id)
        query += " ORDER BY shipment_id, route_code, created_at"
        with self.repository.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def _require_run(self, connection, run_id: str) -> None:
        if not connection.execute("SELECT 1 FROM backtest_runs WHERE run_id=?", (run_id,)).fetchone():
            raise ContractError("回测批次不存在")
