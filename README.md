# otel-tail-sampling-planner
结合错误、延迟和依赖完整性规划 OpenTelemetry 尾部采样

## 定位

这是一个**离线事件模型**的尾部采样规划器:代理只消费按 trace ID 聚合好的
span 事件(最后活跃时间、错误状态、总延迟、关键依赖均由事件推导),**不连接
Collector**、不监听任何网络端口、不依赖 OpenTelemetry SDK。它可以作为 Collector
`tail_sampling` processor 的策略仿真层,用于离线回放、容量规划和策略验证。

## 使用

```python
from tail_sampling_planner import (
    PlannerConfig, Span, SpanStatus, TailSamplingPlanner,
)

config = PlannerConfig(
    wait_window_ms=1000,            # trace 静默多久后必须决策
    latency_budget_ms=500,          # 超过该延迟提升保留优先级
    critical_dependencies={"payments-db"},
    service_budgets={"web": 100, "worker": 50},
    total_budget=120,               # 可选,缺省为各服务预算之和
    min_service_representation=2,   # 每个服务的最低保留名额
)
planner = TailSamplingPlanner(config)
planner.add_span(Span("t1", "s1", "web", None, 0, 100, SpanStatus.OK), now_ms=0)
decisions = planner.collect_decisions(now_ms=0)  # trace 完整 -> 立即决策
```

## 决策规则

- **决策时机**:只有 trace 完整(根 span 到达且所有 parent 均闭合)或等待窗口
  结束时才做出最终决定;窗口内到达的 span 会持续更新 trace 状态。
- **原子化**:保留/丢弃以整条 trace 为单位,不会只保留部分 span。
- **优先级**:错误 > 超预算延迟 > 命中关键依赖 > 普通;同优先级用
  `sha256(trace_id)` 稳定哈希排序,输入顺序不影响结果。
- **配额**:每个服务有独立预算,可选全局总预算;高流量服务无法占满所有名额。
- **低流量保护**:全局竞争后,达不到 `min_service_representation` 的服务会
  置换超配服务中优先级最低的名额,保证最低表示度。

## 边界行为

- **迟到 span(决策后到达)**:trace 已保留则追加进保留集合并计
  `late_spans_kept`;已丢弃则只计 `late_spans_dropped`,不回溯决定。
- **窗口内补齐**:窗口内到达的错误/子 span 正常参与分类,可提升优先级。
- **预算耗尽**:未抢到名额的 trace 丢弃,`reason="budget_exhausted"`。
- **重复 span**:按 `span_id` 去重,计入 `duplicate_spans`。

## 测试

```bash
python -m unittest
```

覆盖正常完成、错误后到、预算争抢、低流量保护、重复 span 和输入乱序。
运行环境 Python 3.8.2+。
