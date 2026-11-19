"""OpenTelemetry 尾部采样(tail sampling)规划器,离线事件模型。

代理只消费按 trace ID 聚合好的离线事件,不连接 Collector。
决策时机:trace 完整(根 span 到达且所有 parent 均已闭合)或等待窗口结束。
保留/丢弃以 trace 为单位原子化执行,不会只保留部分 span。
"""

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set


class SpanStatus(Enum):
    UNSET = "UNSET"
    OK = "OK"
    ERROR = "ERROR"


@dataclass(frozen=True)
class Span:
    trace_id: str
    span_id: str
    service: str
    parent_span_id: Optional[str]
    start_ms: int
    end_ms: int
    status: SpanStatus = SpanStatus.UNSET


class Tier:
    """保留优先级,数值越大越优先。"""

    ERROR = 3
    LATENCY = 2
    CRITICAL = 1
    NORMAL = 0


@dataclass
class Decision:
    trace_id: str
    keep: bool
    reason: str
    service: str
    tier: int
    spans: List[Span] = field(default_factory=list)


@dataclass
class PlannerConfig:
    wait_window_ms: int
    latency_budget_ms: int
    critical_dependencies: Set[str]
    service_budgets: Dict[str, int]
    total_budget: Optional[int] = None
    min_service_representation: int = 1


@dataclass
class PlannerStats:
    duplicate_spans: int = 0
    late_spans_kept: int = 0
    late_spans_dropped: int = 0


def stable_hash(trace_id):
    # type: (str) -> int
    """稳定哈希:同优先级内用它排序,输入顺序不影响结果。"""
    digest = hashlib.sha256(trace_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


class _TraceState:
    __slots__ = ("spans", "last_active_ms")

    def __init__(self):
        self.spans = {}  # type: Dict[str, Span]
        self.last_active_ms = 0

    def is_complete(self):
        # type: () -> bool
        ids = set(self.spans)
        has_root = any(s.parent_span_id is None for s in self.spans.values())
        parents_resolved = all(
            s.parent_span_id is None or s.parent_span_id in ids
            for s in self.spans.values()
        )
        return has_root and parents_resolved

    def root_service(self):
        # type: () -> str
        for span in self.spans.values():
            if span.parent_span_id is None:
                return span.service
        return min(self.spans.values(), key=lambda s: (s.start_ms, s.span_id)).service

    def has_error(self):
        # type: () -> bool
        return any(s.status is SpanStatus.ERROR for s in self.spans.values())

    def duration_ms(self):
        # type: () -> int
        return (
            max(s.end_ms for s in self.spans.values())
            - min(s.start_ms for s in self.spans.values())
        )

    def touches_critical(self, critical):
        # type: (Set[str]) -> bool
        return any(s.service in critical for s in self.spans.values())


class TailSamplingPlanner:
    def __init__(self, config):
        # type: (PlannerConfig) -> None
        self.config = config
        self._pending = {}  # type: Dict[str, _TraceState]
        self._kept = {}  # type: Dict[str, Decision]
        self._dropped = set()  # type: Set[str]
        self.stats = PlannerStats()

    def add_span(self, span, now_ms):
        # type: (Span, int) -> None
        """摄入一个 span。迟到 span:已保留则追加,已丢弃则计入统计且不回溯。"""
        trace_id = span.trace_id
        if trace_id in self._kept:
            decision = self._kept[trace_id]
            if all(s.span_id != span.span_id for s in decision.spans):
                decision.spans.append(span)
                self.stats.late_spans_kept += 1
            else:
                self.stats.duplicate_spans += 1
            return
        if trace_id in self._dropped:
            self.stats.late_spans_dropped += 1
            return

        state = self._pending.setdefault(trace_id, _TraceState())
        if span.span_id in state.spans:
            self.stats.duplicate_spans += 1
            return
        state.spans[span.span_id] = span
        state.last_active_ms = max(state.last_active_ms, now_ms)

    def collect_decisions(self, now_ms):
        # type: (int) -> List[Decision]
        """对窗口结束或已完整的 trace 做出最终决定,按 trace_id 排序返回。"""
        due = [
            trace_id
            for trace_id, state in self._pending.items()
            if state.is_complete()
            or now_ms - state.last_active_ms >= self.config.wait_window_ms
        ]
        if not due:
            return []

        candidates = []
        for trace_id in due:
            state = self._pending.pop(trace_id)
            candidates.append((trace_id, self._classify(trace_id, state), state))

        keep_ids, reasons = self._allocate(candidates)

        decisions = []
        for trace_id, (service, tier), state in candidates:
            keep = trace_id in keep_ids
            decision = Decision(
                trace_id=trace_id,
                keep=keep,
                reason=reasons[trace_id],
                service=service,
                tier=tier,
                spans=sorted(state.spans.values(), key=lambda s: (s.start_ms, s.span_id)),
            )
            if keep:
                self._kept[trace_id] = decision
            else:
                self._dropped.add(trace_id)
            decisions.append(decision)
        decisions.sort(key=lambda d: d.trace_id)
        return decisions

    def _classify(self, trace_id, state):
        # type: (str, _TraceState) -> tuple
        service = state.root_service()
        if state.has_error():
            tier = Tier.ERROR
        elif state.duration_ms() > self.config.latency_budget_ms:
            tier = Tier.LATENCY
        elif state.touches_critical(self.config.critical_dependencies):
            tier = Tier.CRITICAL
        else:
            tier = Tier.NORMAL
        return service, tier

    def _allocate(self, candidates):
        # type: (list) -> tuple
        """先按 (优先级, 稳定哈希) 全局竞争,再为低于保底的服务置换名额。"""
        cfg = self.config
        total_budget = (
            cfg.total_budget
            if cfg.total_budget is not None
            else sum(cfg.service_budgets.values())
        )

        def sort_key(item):
            trace_id, (service, tier), _ = item
            return (-tier, stable_hash(trace_id))

        by_service = {}  # type: Dict[str, list]
        for item in candidates:
            by_service.setdefault(item[1][0], []).append(item)
        for items in by_service.values():
            items.sort(key=sort_key)

        keep_ids = set()  # type: Set[str]
        reasons = {}
        used = {service: 0 for service in by_service}
        arrived = {service: len(items) for service, items in by_service.items()}

        def floor_for(service):
            return min(
                cfg.min_service_representation,
                cfg.service_budgets.get(service, 0),
                arrived[service],
            )

        def keep(item, reason):
            trace_id, (service, tier), _ = item
            keep_ids.add(trace_id)
            reasons[trace_id] = reason
            used[service] += 1

        def unkeep(item):
            trace_id, (service, tier), _ = item
            keep_ids.discard(trace_id)
            reasons[trace_id] = "budget_exhausted"
            used[service] -= 1

        tier_reason = {
            Tier.ERROR: "error",
            Tier.LATENCY: "latency_over_budget",
            Tier.CRITICAL: "critical_dependency",
            Tier.NORMAL: "normal",
        }

        # 第一轮:全局按优先级与稳定哈希竞争,受服务配额与总预算约束。
        ordered = sorted(candidates, key=sort_key)
        for item in ordered:
            service = item[1][0]
            if len(keep_ids) >= total_budget:
                break
            if used[service] >= cfg.service_budgets.get(service, 0):
                continue
            keep(item, tier_reason[item[1][1]])

        # 第二轮:低流量服务达不到最低表示度时,置换超配服务中优先级最低的名额。
        by_id = {item[0]: item for item in candidates}
        for service, items in by_service.items():
            for item in items:
                if used[service] >= floor_for(service):
                    break
                if item[0] in keep_ids:
                    continue
                if len(keep_ids) >= total_budget:
                    victim = self._eviction_candidate(
                        by_id, keep_ids, floor_for, used)
                    if victim is None:
                        break
                    unkeep(victim)
                if used[service] >= cfg.service_budgets.get(service, 0):
                    break
                keep(item, "min_representation")

        for item in candidates:
            reasons.setdefault(item[0], "budget_exhausted")
        return keep_ids, reasons

    @staticmethod
    def _eviction_candidate(by_id, keep_ids, floor_for, used):
        """在超出自身保底的服务中,挑优先级最低、哈希最大的已保留 trace。"""
        candidates = []
        for trace_id in keep_ids:
            item = by_id[trace_id]
            service, tier = item[1]
            if used[service] > floor_for(service):
                candidates.append(item)
        if not candidates:
            return None
        return min(candidates, key=lambda item: (item[1][1], -stable_hash(item[0])))
