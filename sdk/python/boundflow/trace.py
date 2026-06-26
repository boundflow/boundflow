"""Run-trace capture — OTel-shaped spans for every LLM call and tool call.

The worker runs customer-side, so traces (which contain the full prompt/response
and tool input/output) can land in the customer's *own* store — their database,
an OpenTelemetry backend, a file — and never have to leave their infra. You wire
that up by passing a TraceSink to BoundFlowWorker(trace_sink=...).

Field names follow OpenTelemetry GenAI semantic conventions where they apply, so
OTelTraceSink maps cleanly onto any OTLP backend. The trace_id is the run id
(request_id): every span of one workflow invocation — across agents, operations,
even an approval pause — groups into a single trace, while `agent`/`workflow_type`
let you slice one agent's behavior across many runs.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol, runtime_checkable

log = logging.getLogger("boundflow.trace")


def now_ms() -> int:
    return int(time.time() * 1000)


# ── OpenTelemetry GenAI semantic-convention attribute keys ────────────────────
# Centralized so a convention bump (these are still experimental) is a one-line
# edit, and producer/sink/tests can't drift on a typo'd key. Mirrors the keys
# from opentelemetry-semconv without taking the dependency (OTel stays optional).
GEN_AI_OPERATION_NAME = "gen_ai.operation.name"
GEN_AI_SYSTEM = "gen_ai.system"
GEN_AI_CONVERSATION_ID = "gen_ai.conversation.id"
GEN_AI_REQUEST_MODEL = "gen_ai.request.model"
GEN_AI_REQUEST_MAX_TOKENS = "gen_ai.request.max_tokens"
GEN_AI_USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS = "gen_ai.usage.cache_creation_input_tokens"
GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS = "gen_ai.usage.cache_read_input_tokens"
GEN_AI_RESPONSE_FINISH_REASONS = "gen_ai.response.finish_reasons"
GEN_AI_INPUT_MESSAGES = "gen_ai.input.messages"
GEN_AI_OUTPUT_MESSAGES = "gen_ai.output.messages"
GEN_AI_TOOL_NAME = "gen_ai.tool.name"
GEN_AI_TOOL_CALL_ARGUMENTS = "gen_ai.tool.call.arguments"
GEN_AI_TOOL_CALL_RESULT = "gen_ai.tool.call.result"
GEN_AI_TOOL_CALL_ID = "gen_ai.tool.call.id"
GEN_AI_TOOL_DESCRIPTION = "gen_ai.tool.description"
GEN_AI_AGENT_NAME = "gen_ai.agent.name"
# BoundFlow-specific attribute keys (not part of the GenAI spec):
BF_COST_USD = "boundflow.cost_usd"
BF_RUN_ID = "boundflow.run_id"
BF_WORKFLOW_ID = "boundflow.workflow_id"
BF_WORKFLOW_TYPE = "boundflow.workflow_type"
BF_WORKFLOW_VERSION = "boundflow.workflow_version"
BF_OPERATION = "boundflow.operation"
BF_OUTCOME = "boundflow.outcome"
BF_FAILED = "boundflow.failed"

# ── Vocabulary values (not attribute keys) ────────────────────────────────────
# GenAI operation-name values (the value of gen_ai.operation.name):
GEN_AI_OP_CHAT = "chat"
GEN_AI_OP_EXECUTE_TOOL = "execute_tool"
GEN_AI_OP_INVOKE_AGENT = "invoke_agent"
# Chat message roles:
ROLE_SYSTEM = "system"
ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"
ROLE_TOOL = "tool"
# GenAI message part types:
PART_TEXT = "text"
PART_TOOL_CALL = "tool_call"
PART_TOOL_CALL_RESPONSE = "tool_call_response"
# BoundFlow span kinds:
SPAN_KIND_LLM = "llm"
SPAN_KIND_TOOL = "tool"
# Operation outcomes:
OUTCOME_COMPLETED = "completed"
OUTCOME_NEXT = "next"
OUTCOME_AWAIT_APPROVAL = "await_approval"
# Generic error attribute keys:
ERROR = "error"
ERROR_MESSAGE = "error.message"


@dataclass
class Span:
    """One unit of work within an agent run: an LLM call ("llm") or a tool call
    ("tool"). `attributes` carries GenAI-conventioned keys for OTel export."""
    kind: str
    name: str
    start_ms: int
    end_ms: int
    input: Any = None      # llm: messages sent; tool: arguments
    output: Any = None     # llm: response content; tool: result
    error: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms


@dataclass
class AgentRunTrace:
    """One agent run (one OperationContext.run_agent call): its ordered LLM + tool
    spans and rollup metrics. Nested inside an OperationTrace, which carries the
    run/workflow identity."""
    agent: str
    model: str
    start_ms: int
    end_ms: int
    spans: list[Span]
    output: Any = None
    cost_usd: float = 0.0
    tokens: int = 0
    llm_calls: int = 0


@dataclass
class OperationTrace:
    """The emitted unit: one atomic operation (one worker dispatch). Carries the
    operation outcome + whether the run was marked failed, and the agent runs that
    happened inside it. Every operation of one workflow invocation shares
    trace_id (= request_id), so the backend assembles them into one 'workflow run';
    slice by `agent` across runs for one agent's behavior over time."""
    trace_id: str          # = request_id (the workflow invocation / run)
    workflow_id: str
    workflow_type: str
    version: int
    operation: str         # operation name, e.g. "invoke_entry"
    outcome: str           # "completed" | "next" | "await_approval"
    failed: bool           # the handler called ctx.mark_failed()
    start_ms: int
    end_ms: int
    agent_runs: list[AgentRunTrace]

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


@runtime_checkable
class TraceSink(Protocol):
    """Where traces go. Implement this to ship operations to your own store
    (Postgres, an OTel backend, a queue…). The worker guards emit() so a failure
    never breaks the run, but keep it cheap and resilient."""
    async def emit(self, trace: OperationTrace) -> None: ...


class LoggingTraceSink:
    """Logs a one-line summary per operation; full=True logs the whole trace as JSON."""
    def __init__(self, *, full: bool = False, level: int = logging.INFO) -> None:
        self._full = full
        self._level = level

    async def emit(self, trace: OperationTrace) -> None:
        if self._full:
            log.log(self._level, "trace %s", trace.to_json())
        else:
            spans = sum(len(r.spans) for r in trace.agent_runs)
            cost = sum(r.cost_usd for r in trace.agent_runs)
            log.log(self._level,
                    "run=%s op=%s outcome=%s failed=%s agents=%d spans=%d cost=$%.4f",
                    trace.trace_id, trace.operation, trace.outcome, trace.failed,
                    len(trace.agent_runs), spans, cost)


class JsonlFileTraceSink:
    """Appends one JSON line per operation to a file — the simplest 'bring your own
    storage' sink. Tail it, ship it, or load it into anything."""
    def __init__(self, path: str) -> None:
        self._path = path

    async def emit(self, trace: OperationTrace) -> None:
        with open(self._path, "a") as f:
            f.write(trace.to_json() + "\n")


class OTelTraceSink:
    """Maps each run onto OpenTelemetry spans (parent = the agent run, children =
    LLM/tool calls) with GenAI semantic-convention attributes, exported through
    whatever OTel tracer/exporter you've configured (e.g. OTLP to your collector).

    Requires the optional dependency: `pip install 'boundflow[otel]'`.
    """
    def __init__(self, tracer: Any | None = None) -> None:
        try:
            from opentelemetry import trace as _ot
            from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
        except ImportError as e:  # pragma: no cover - optional dep
            raise ImportError(
                "OTelTraceSink requires opentelemetry; install with: pip install 'boundflow[otel]'"
            ) from e
        self._ot = _ot
        self._NonRecordingSpan = NonRecordingSpan
        self._SpanContext = SpanContext
        self._TraceFlags = TraceFlags
        self._tracer = tracer or _ot.get_tracer("boundflow")

    @staticmethod
    def _attr(v: Any) -> Any:
        return v if isinstance(v, (str, int, float, bool)) else json.dumps(v, default=str)

    def _run_context(self, request_id: str):
        """A parent context whose OTel trace_id is derived deterministically from
        the run id, so every operation of one invocation lands in ONE OTel trace —
        which is what makes a multi-operation run render as a single trace in OTel
        backends (Langfuse, Phoenix, …), not N separate ones."""
        h = hashlib.sha256(request_id.encode()).digest()
        trace_id = int.from_bytes(h[:16], "big") or 1
        span_id = int.from_bytes(h[16:24], "big") or 1
        sc = self._SpanContext(
            trace_id=trace_id, span_id=span_id, is_remote=True,
            trace_flags=self._TraceFlags(self._TraceFlags.SAMPLED),
        )
        return self._ot.set_span_in_context(self._NonRecordingSpan(sc))

    async def emit(self, trace: OperationTrace) -> None:
        parent = self._run_context(trace.trace_id)
        op = self._tracer.start_span(f"operation {trace.operation}", context=parent,
                                     start_time=trace.start_ms * 1_000_000)
        op.set_attribute(BF_RUN_ID, trace.trace_id)
        op.set_attribute(BF_WORKFLOW_ID, trace.workflow_id)
        op.set_attribute(BF_WORKFLOW_TYPE, trace.workflow_type)
        op.set_attribute(BF_WORKFLOW_VERSION, trace.version)
        op.set_attribute(GEN_AI_CONVERSATION_ID, trace.workflow_id)
        op.set_attribute(BF_OPERATION, trace.operation)
        op.set_attribute(BF_OUTCOME, trace.outcome)
        op.set_attribute(BF_FAILED, trace.failed)
        op_ctx = self._ot.set_span_in_context(op)
        for run in trace.agent_runs:
            agent = self._tracer.start_span(f"agent {run.agent}", context=op_ctx,
                                            start_time=run.start_ms * 1_000_000)
            agent.set_attribute(GEN_AI_OPERATION_NAME, GEN_AI_OP_INVOKE_AGENT)
            agent.set_attribute(GEN_AI_AGENT_NAME, run.agent)
            agent.set_attribute(GEN_AI_REQUEST_MODEL, run.model)
            agent.set_attribute(BF_COST_USD, run.cost_usd)
            agent_ctx = self._ot.set_span_in_context(agent)
            for s in run.spans:
                child = self._tracer.start_span(s.name, context=agent_ctx, start_time=s.start_ms * 1_000_000)
                for k, v in s.attributes.items():
                    child.set_attribute(k, self._attr(v))
                # Content as GenAI-conventioned attributes (input/output are already
                # in the canonical message shape; tools render them directly).
                if s.kind == SPAN_KIND_LLM:
                    if s.input is not None:
                        child.set_attribute(GEN_AI_INPUT_MESSAGES, json.dumps(s.input, default=str))
                    if s.output is not None:
                        child.set_attribute(GEN_AI_OUTPUT_MESSAGES, json.dumps(s.output, default=str))
                elif s.kind == SPAN_KIND_TOOL:
                    if s.input is not None:
                        child.set_attribute(GEN_AI_TOOL_CALL_ARGUMENTS, json.dumps(s.input, default=str))
                    if s.output is not None:
                        child.set_attribute(GEN_AI_TOOL_CALL_RESULT, json.dumps(s.output, default=str))
                if s.error:
                    child.set_attribute(ERROR, True)
                    child.set_attribute(ERROR_MESSAGE, s.error)
                child.end(end_time=s.end_ms * 1_000_000)
            agent.end(end_time=run.end_ms * 1_000_000)
        op.end(end_time=trace.end_ms * 1_000_000)
