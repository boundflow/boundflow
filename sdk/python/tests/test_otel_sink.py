"""OTelTraceSink — maps OperationTraces onto OpenTelemetry spans.

Verifies the caveat-1 fix: every operation of one run shares a single OTel
trace_id (derived from request_id), so a multi-operation invocation renders as
ONE trace in OTel backends, not N. Pure unit test, no backend.
"""
from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry")

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from boundflow.trace import AgentRunTrace, OTelTraceSink, OperationTrace, Span


def _op(trace_id: str, operation: str) -> OperationTrace:
    span = Span(kind="llm", name="chat claude-x", start_ms=1, end_ms=2,
                attributes={"gen_ai.request.model": "claude-x", "gen_ai.system": "anthropic"})
    run = AgentRunTrace(agent="analyst", model="claude-x", start_ms=1, end_ms=3, spans=[span])
    return OperationTrace(trace_id=trace_id, workflow_id="wf1", workflow_type="t", version=1,
                          operation=operation, outcome="completed", failed=False,
                          start_ms=1, end_ms=4, agent_runs=[run])


def _sink_with_exporter():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return OTelTraceSink(provider.get_tracer("test")), exporter


async def test_operations_of_one_run_share_a_single_otel_trace():
    sink, exporter = _sink_with_exporter()

    # Two operations of the SAME run (same request_id) ...
    await sink.emit(_op("run-7", "invoke_entry"))
    await sink.emit(_op("run-7", "followup"))
    spans = exporter.get_finished_spans()
    assert len({s.context.trace_id for s in spans}) == 1, \
        "all operations of one run must land in a single OTel trace"

    # ... and a different run is a different trace.
    await sink.emit(_op("run-8", "invoke_entry"))
    assert len({s.context.trace_id for s in exporter.get_finished_spans()}) == 2


async def test_span_hierarchy_and_genai_attributes():
    sink, exporter = _sink_with_exporter()
    await sink.emit(_op("run-9", "invoke_entry"))
    spans = exporter.get_finished_spans()

    op = next(s for s in spans if s.name.startswith("operation"))
    agent = next(s for s in spans if s.name.startswith("agent"))
    chat = next(s for s in spans if s.name.startswith("chat"))

    # operation -> agent -> chat nesting, all in one trace
    assert agent.parent.span_id == op.context.span_id
    assert chat.parent.span_id == agent.context.span_id

    # GenAI attributes the agent-observability tools key on
    assert agent.attributes["gen_ai.operation.name"] == "invoke_agent"
    assert agent.attributes["gen_ai.agent.name"] == "analyst"
    assert chat.attributes["gen_ai.request.model"] == "claude-x"
    assert chat.attributes["gen_ai.system"] == "anthropic"
    assert op.attributes["gen_ai.conversation.id"] == "wf1"
