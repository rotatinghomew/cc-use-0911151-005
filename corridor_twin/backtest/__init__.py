"""滚动回测子系统：基于事件重放的到达时间预测验证。

设计原则：
- 严格的决策时点可见性：任何预测只能使用 occurred_at <= 决策时刻 的事件版本；
- 事件更正以新版本进入事件流，旧版本保留用于事后审计，不覆盖历史；
- 不可达、改道、冷链异常等旅程始终保留在统计分母中，绝不静默剔除。
"""
from corridor_twin.backtest.event_stream import StreamEvent, load_event_stream, ReplayView
from corridor_twin.backtest.eta_forecast import Forecast, EtaForecaster, MODEL_VERSION
from corridor_twin.backtest.rolling import RollingBacktest, BacktestResult
from corridor_twin.backtest.recommendation import Recommendation, recommend, evaluate_eligibility
from corridor_twin.backtest.store import BacktestStore
from corridor_twin.backtest.service import BacktestApplication

__all__ = [
    "StreamEvent",
    "load_event_stream",
    "ReplayView",
    "Forecast",
    "EtaForecaster",
    "MODEL_VERSION",
    "RollingBacktest",
    "BacktestResult",
    "Recommendation",
    "recommend",
    "evaluate_eligibility",
    "BacktestStore",
    "BacktestApplication",
]
