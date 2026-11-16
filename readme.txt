中国—东盟跨境智慧物流走廊数字孪生

transport_book 提供手工路段容量和运输承诺。capacity 按路段与时间窗登记容量及版本，
reserve 在一个事务内校验连续路径、货票状态、各路段版本和合计占用；同一货票不能重复预订。
transition 使承诺进入 dispatched、completed 或尚未发运时 cancelled，完成或取消释放占用，
已发运承诺不可撤销。发运前货票资料改变需重新确认。inspect 可查看各时窗剩余容量及历史承诺。

运行验收：
python3 -m corridor_twin run --fixture fixtures/acceptance.json --database /tmp/corridor_twin.sqlite3 --output /tmp/corridor_twin-output

核验：
python3 -m corridor_twin verify --database /tmp/corridor_twin.sqlite3 --output /tmp/corridor_twin-output

全量测试：
python3 -m unittest discover -s tests -v

按模块测试：
python3 -m unittest tests.events.test_logistics_event -v

构建检查：
python3 -m compileall -q corridor_twin tests

ETA 滚动回测（corridor_twin.backtest）：
backtest 基于不可变事件流做滚动回测，验证候选线路在不同口岸、货类上的到达时间预测
误差与不确定性。ReplayView 严格按 occurred_at 截断，任何预测只能使用决策时点已经到达
的事件版本；事后修订的路况、容量不会回溯美化历史预测。事件更正（is_correction=true、
revision 递增）会使受影响且尚未发出的预测失效（superseded）并基于更正后的可见知识重算；
已采纳（adopted）的运输预测冻结保留，只允许事后比较误差。不可达、改道、冷链异常旅程
始终计入口径，绝不静默剔除。推荐需通过最小样本量、MAE、覆盖率与不确定性包络门槛，
并携带样本旅程、指标、forecast_id 与 knowledge_fingerprint 以便在实际路线建议中回溯。

运行回测并导出证据包：
python3 -m corridor_twin backtest --fixture fixtures/backtest_events.json --database /tmp/bt.sqlite3 --output /tmp/bt-output
python3 -m corridor_twin backtest-verify --output /tmp/bt-output

回测夹具由 scripts/generate_backtest_fixture.py 确定性生成，覆盖两个口岸×两类货、
正常送达与三类异常结局，以及一条路况事后被更正的未结束旅程。
