"""AgentInvocationMetrics.latency_seconds — the wall-clock duration of one agent
invocation (run_agent), summed server-side into the workflow's rolling latency
metric (WorkflowMetric.LATENCY). worker.py has always computed the duration around
run_step, but metrics_to_proto never put it on the wire — this pins the fix.
No live-server surface exists to read TotalLatencySeconds back today, so this
exercises the conversion directly rather than end-to-end."""
from __future__ import annotations

from boundflow._transport import metrics_to_proto


def test_metrics_to_proto_carries_latency_seconds():
    proto = metrics_to_proto({
        "cost_usd": 0.01,
        "llm_calls": 1,
        "tokens_used": 100,
        "latency_seconds": 2.5,
        "ran_at": 1700000000000,
    })
    assert proto.HasField("latency_seconds")
    assert proto.latency_seconds == 2.5


def test_metrics_to_proto_defaults_latency_seconds_when_absent():
    proto = metrics_to_proto({"cost_usd": 0.01, "llm_calls": 1, "tokens_used": 100})
    assert proto.HasField("latency_seconds")
    assert proto.latency_seconds == 0.0
