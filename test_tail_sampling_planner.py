import unittest

from tail_sampling_planner import (
    PlannerConfig,
    Span,
    SpanStatus,
    TailSamplingPlanner,
    Tier,
    stable_hash,
)


def make_config(**overrides):
    params = dict(
        wait_window_ms=1000,
        latency_budget_ms=500,
        critical_dependencies={"payments-db"},
        service_budgets={"web": 10, "worker": 10},
        min_service_representation=1,
    )
    params.update(overrides)
    return PlannerConfig(**params)


def root_span(trace_id, service="web", start=0, end=100, status=SpanStatus.OK):
    return Span(trace_id, trace_id + "-root", service, None, start, end, status)


def child_span(trace_id, span_id, service, parent, start=10, end=50,
               status=SpanStatus.OK):
    return Span(trace_id, span_id, service, parent, start, end, status)


def kept_ids(decisions):
    return {d.trace_id for d in decisions if d.keep}


class NormalCompletionTest(unittest.TestCase):
    def test_complete_trace_decided_immediately_and_atomically(self):
        planner = TailSamplingPlanner(make_config())
        planner.add_span(root_span("t1"), now_ms=100)
        planner.add_span(
            child_span("t1", "t1-child", "payments-db", "t1-root"), now_ms=120)

        decisions = planner.collect_decisions(now_ms=120)
        self.assertEqual(len(decisions), 1)
        decision = decisions[0]
        self.assertTrue(decision.keep)
        self.assertEqual(decision.reason, "critical_dependency")
        self.assertEqual(decision.tier, Tier.CRITICAL)
        # 原子化:trace 的全部 span 一起保留。
        self.assertEqual({s.span_id for s in decision.spans},
                         {"t1-root", "t1-child"})

    def test_incomplete_trace_waits_for_window(self):
        planner = TailSamplingPlanner(make_config())
        # 子 span 先到、根 span 未到,parent 未闭合,trace 不完整。
        planner.add_span(
            child_span("t1", "t1-child", "web", "t1-root"), now_ms=0)
        self.assertEqual(planner.collect_decisions(now_ms=500), [])
        # 窗口结束后即使不完整也必须做出决定。
        decisions = planner.collect_decisions(now_ms=1000)
        self.assertEqual(len(decisions), 1)
        self.assertTrue(decisions[0].keep)
        self.assertEqual({s.span_id for s in decisions[0].spans}, {"t1-child"})

    def test_completion_within_window_supersedes_waiting(self):
        planner = TailSamplingPlanner(make_config())
        planner.add_span(root_span("t1"), now_ms=0)
        planner.add_span(
            child_span("t1", "t1-child", "web", "t1-root"), now_ms=300)
        decisions = planner.collect_decisions(now_ms=300)
        self.assertEqual(len(decisions), 1)


class LateErrorTest(unittest.TestCase):
    def test_error_span_arriving_within_window_boosts_priority(self):
        config = make_config(service_budgets={"web": 1})
        planner = TailSamplingPlanner(config)
        planner.add_span(root_span("normal"), now_ms=0)
        planner.add_span(root_span("will-error"), now_ms=10)
        # 窗口内两个 trace 的子 span 都补齐,错误 trace 升级为最高优先级。
        planner.add_span(
            child_span("normal", "n-child", "web", "normal-root"), now_ms=200)
        planner.add_span(
            child_span("will-error", "we-child", "web", "will-error-root",
                       status=SpanStatus.ERROR),
            now_ms=200)
        decisions = planner.collect_decisions(now_ms=200)
        self.assertEqual(kept_ids(decisions), {"will-error"})
        by_id = {d.trace_id: d for d in decisions}
        self.assertEqual(by_id["will-error"].reason, "error")
        self.assertEqual(by_id["normal"].reason, "budget_exhausted")

    def test_late_span_after_drop_is_not_resurrected(self):
        config = make_config(service_budgets={"web": 1})
        planner = TailSamplingPlanner(config)
        planner.add_span(root_span("a"), now_ms=0)
        planner.add_span(root_span("b"), now_ms=0)
        decisions = planner.collect_decisions(now_ms=0)
        dropped = [d.trace_id for d in decisions if not d.keep]
        self.assertEqual(len(dropped), 1)

        # 已丢弃 trace 的迟到错误 span:只计统计,不改变决定。
        planner.add_span(
            Span(dropped[0], "late", "web", dropped[0] + "-root", 5, 20,
                 SpanStatus.ERROR),
            now_ms=50)
        self.assertEqual(planner.stats.late_spans_dropped, 1)
        self.assertEqual(planner.collect_decisions(now_ms=50), [])

    def test_late_span_after_keep_is_appended(self):
        planner = TailSamplingPlanner(make_config())
        planner.add_span(root_span("t1"), now_ms=0)
        planner.collect_decisions(now_ms=0)
        planner.add_span(
            Span("t1", "late", "web", "t1-root", 5, 20, SpanStatus.ERROR),
            now_ms=50)
        self.assertEqual(planner.stats.late_spans_kept, 1)
        kept = planner._kept["t1"]
        self.assertIn("late", {s.span_id for s in kept.spans})


class BudgetContentionTest(unittest.TestCase):
    def test_error_beats_latency_beats_normal(self):
        config = make_config(service_budgets={"web": 2})
        planner = TailSamplingPlanner(config)
        planner.add_span(root_span("err", status=SpanStatus.ERROR), now_ms=0)
        planner.add_span(root_span("slow", start=0, end=900), now_ms=0)
        planner.add_span(root_span("ok"), now_ms=0)
        decisions = planner.collect_decisions(now_ms=0)
        self.assertEqual(kept_ids(decisions), {"err", "slow"})

    def test_same_tier_tie_broken_by_stable_hash(self):
        config = make_config(service_budgets={"web": 2})
        planner = TailSamplingPlanner(config)
        trace_ids = ["t-%d" % i for i in range(5)]
        for tid in trace_ids:
            planner.add_span(root_span(tid), now_ms=0)
        decisions = planner.collect_decisions(now_ms=0)
        expected = set(sorted(trace_ids, key=stable_hash)[:2])
        self.assertEqual(kept_ids(decisions), expected)
        dropped = [d for d in decisions if not d.keep]
        self.assertTrue(all(d.reason == "budget_exhausted" for d in dropped))

    def test_per_service_quota_is_enforced(self):
        config = make_config(service_budgets={"web": 1, "worker": 1})
        planner = TailSamplingPlanner(config)
        planner.add_span(root_span("w1", service="web"), now_ms=0)
        planner.add_span(root_span("w2", service="web"), now_ms=0)
        planner.add_span(root_span("k1", service="worker"), now_ms=0)
        decisions = planner.collect_decisions(now_ms=0)
        kept = kept_ids(decisions)
        self.assertEqual(len(kept), 2)
        self.assertIn("k1", kept)
        self.assertEqual(len(kept & {"w1", "w2"}), 1)


class LowTrafficProtectionTest(unittest.TestCase):
    def test_min_representation_survives_global_budget_pressure(self):
        # 全局预算 4,高流量服务全是错误 trace;低流量服务仍有保底名额。
        config = make_config(
            service_budgets={"busy": 10, "rare": 10},
            total_budget=4,
            min_service_representation=2,
        )
        planner = TailSamplingPlanner(config)
        for i in range(5):
            planner.add_span(
                root_span("busy-%d" % i, service="busy",
                          status=SpanStatus.ERROR),
                now_ms=0)
        for i in range(2):
            planner.add_span(root_span("rare-%d" % i, service="rare"), now_ms=0)
        decisions = planner.collect_decisions(now_ms=0)
        kept = kept_ids(decisions)
        self.assertIn("rare-0", kept)
        self.assertIn("rare-1", kept)
        self.assertEqual(len(kept), 4)
        self.assertEqual(
            len([d for d in decisions if d.keep and d.service == "busy"]), 2)
        rare = [d for d in decisions if d.service == "rare"]
        self.assertTrue(all(d.reason == "min_representation" for d in rare))


class DuplicateAndOrderTest(unittest.TestCase):
    def test_duplicate_span_ignored(self):
        planner = TailSamplingPlanner(make_config())
        span = root_span("t1")
        planner.add_span(span, now_ms=0)
        planner.add_span(span, now_ms=10)
        planner.add_span(span, now_ms=20)
        self.assertEqual(planner.stats.duplicate_spans, 2)
        decisions = planner.collect_decisions(now_ms=0)
        self.assertEqual(len(decisions[0].spans), 1)

    def test_input_order_does_not_change_outcome(self):
        trace_ids = ["t-%d" % i for i in range(8)]

        def run(order):
            config = make_config(service_budgets={"web": 3})
            planner = TailSamplingPlanner(config)
            for index, tid in enumerate(order):
                planner.add_span(root_span(tid), now_ms=index)
            decisions = planner.collect_decisions(now_ms=len(order))
            return kept_ids(decisions)

        forward = run(trace_ids)
        reversed_order = run(list(reversed(trace_ids)))
        shuffled = run([trace_ids[i] for i in (3, 0, 7, 1, 5, 2, 6, 4)])
        self.assertEqual(forward, reversed_order)
        self.assertEqual(forward, shuffled)

    def test_interleaved_spans_across_traces(self):
        config = make_config(service_budgets={"web": 1})
        planner = TailSamplingPlanner(config)
        planner.add_span(root_span("x"), now_ms=0)
        planner.add_span(root_span("y", status=SpanStatus.ERROR), now_ms=1)
        planner.add_span(
            child_span("x", "x-c", "web", "x-root"), now_ms=2)
        planner.add_span(
            child_span("y", "y-c", "web", "y-root", status=SpanStatus.ERROR),
            now_ms=3)
        decisions = planner.collect_decisions(now_ms=3)
        self.assertEqual(kept_ids(decisions), {"y"})
        kept = [d for d in decisions if d.keep][0]
        self.assertEqual({s.span_id for s in kept.spans}, {"y-root", "y-c"})


if __name__ == "__main__":
    unittest.main()
