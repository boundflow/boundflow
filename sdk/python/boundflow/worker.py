"""Worker-side surface: agent definitions, tools, operation handlers.

Mirrors BoundFlow.SDK.BoundFlowWorker + OperationContext, made Python-native:
decorators for registration, plain async functions for tools and handlers.
"""

from __future__ import annotations

import inspect
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Union

from .errors import PlatformError
from .lifecycle import (
    apply_lifecycle_rules,
    load_history,
    load_lifecycle_rules,
    load_runtime_policy,
)
from .llm import AgentStepConfig, LlmClient, Orchestrator, StepResult
from .trace import (
    OUTCOME_AWAIT_APPROVAL,
    OUTCOME_COMPLETED,
    OUTCOME_NEXT,
    AgentRunTrace,
    OperationTrace,
    TraceSink,
    now_ms,
)

log = logging.getLogger("boundflow.worker")

# ── Tools ────────────────────────────────────────────────────────────────────

ToolHandler = Callable[[dict], Awaitable[Any]]


@dataclass
class Tool:
    name: str
    description: str
    handler: ToolHandler
    mode: str | None = None
    input_schema: dict | None = None


def tool(
    fn: ToolHandler | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    mode: str | None = None,
    input_schema: dict | None = None,
) -> Tool | Callable[[ToolHandler], Tool]:
    """Turn an async function into a Tool. Usable bare (`@tool`) or with args.

    The docstring becomes the description the model sees, unless overridden.
    """

    def wrap(f: ToolHandler) -> Tool:
        return Tool(
            name=name or f.__name__,
            description=description or (inspect.getdoc(f) or f.__name__),
            handler=f,
            mode=mode,
            input_schema=input_schema,
        )

    return wrap(fn) if fn is not None else wrap


@dataclass
class AgentDefinition:
    name: str
    system_prompt: str
    model: str
    tools: list[Tool] = field(default_factory=list)
    output_schema: dict | None = None
    cache: bool = False  # opt-in prompt caching of the stable prefix (system + tools)


# ── Operation results ────────────────────────────────────────────────────────


@dataclass
class Complete:
    """The operation is done. `result` is optional — the run's published output,
    persisted on the request and readable later via get_request_info().result."""

    result: dict | None = None


@dataclass
class Next:
    """Advance to another operation with fresh context."""

    operation: str
    context: dict
    timeout: int


@dataclass
class AwaitApproval:
    """Park for human approval; branch on the decision."""

    on_approve: "OperationResult"
    on_reject: "OperationResult"
    timeout: int
    justification: str | None = None


OperationResult = Union[Complete, Next, AwaitApproval]


# ── Operation context (handed to every handler) ──────────────────────────────


class OperationContext:
    def __init__(self, operation: Any, orchestrator: Orchestrator,
                 sink: TraceSink | None = None) -> None:
        self._op = operation
        self._orchestrator = orchestrator
        self._sink = sink
        self._agent_runs: list[AgentRunTrace] = []  # accumulated for the operation trace
        self._llm_context: list[tuple[str, str, Any]] = []  # (key, metadata, payload)
        self.failed = False
        # Per-agent metrics from this operation, sent back to the server in the
        # AtomicOperationResult. Keyed by agent name. (Read by the worker stream.)
        self.agent_state_updates: dict[str, dict] = {}
        # Per-agent lifecycle policy actions applied this operation (only when the
        # rules changed the effective policy). Keyed by agent name; the server audits
        # each. Values: {base_policy, effective_policy, fired_rules:[(rule, value)]}.
        self.agent_policy_actions: dict[str, dict] = {}

    @property
    def name(self) -> str:
        return self._op.name

    @property
    def workflow_version(self) -> int:
        return self._op.workflow_version

    @property
    def context(self) -> dict:
        """The operation's context — the caller's own keys, read and written freely
        (seeded by invoke_workflow(context=...) and carried across operations). The
        runtime's own keys live outside this view, so it's just the customer's data."""
        raw = self._op.context
        if not isinstance(raw.get("input"), dict):
            raw["input"] = {}
        return raw["input"]

    def add_context(self, metadata: str, payload: Any, *, key: str | None = None) -> "OperationContext":
        self._llm_context.append((key or metadata, metadata, payload))
        return self

    def mark_failed(self) -> None:
        """Flag this run as a customer-side failure (increments num_failures)."""
        self.failed = True

    async def run_agent(self, agent: AgentDefinition) -> StepResult:
        """Run an agent step. Runtime policy is snapshotted at request-creation
        time; lifecycle policy + metrics history are injected by the scheduler.
        Lifecycle rules are evaluated before the run; metrics are written back on
        completion. Port of BoundFlowWorker.RunAgentAsync."""
        runtime_node = (self._op.context.get("agentRuntimePolicies") or {}).get(agent.name)
        state_node = (self._op.context.get("agentStates") or {}).get(agent.name)

        base_policy = load_runtime_policy(runtime_node)
        rules = load_lifecycle_rules(state_node)
        history = load_history(state_node)

        # Evaluate lifecycle rules; they may change the effective runtime policy.
        runtime_policy, fired = apply_lifecycle_rules(rules, history, base_policy)

        # Audit the firing only when it actually changed the policy (effective != base).
        if fired and runtime_policy != base_policy:
            self.agent_policy_actions[agent.name] = {
                "base_policy": base_policy,
                "effective_policy": runtime_policy,
                "fired_rules": fired,
            }

        effective_model = runtime_policy.model or agent.model

        cfg = AgentStepConfig(
            objective=agent.name,
            system_prompt=agent.system_prompt,
            policy=runtime_policy,
            model=effective_model,
            tools=agent.tools,
            output_schema=agent.output_schema,
            llm_context=self._llm_context,
            pricing=(self._op.context.get("modelPricing") or {}),
            cache=agent.cache,
        )

        _run_start = now_ms()
        result = await self._orchestrator.run_step(cfg)
        _run_end = now_ms()

        # Emit this run's snapshot; the server appends it to invocation_metrics.
        self.agent_state_updates[agent.name] = {
            "cost_usd": result.cost_usd,
            "llm_calls": result.llm_calls_used,
            "tokens_used": result.tokens_used,
            "calls_per_tool": dict(result.calls_per_tool),
            "tool_failure_counts": dict(result.tool_failure_counts),
            "ran_at": int(time.time() * 1000),
        }
        if self._sink is not None:
            self._agent_runs.append(AgentRunTrace(
                agent=agent.name, model=effective_model,
                start_ms=_run_start, end_ms=_run_end,
                spans=result.spans, output=result.output,
                cost_usd=result.cost_usd, tokens=result.tokens_used,
                llm_calls=result.llm_calls_used,
            ))
        return result


HandlerFn = Callable[[OperationContext], Awaitable[OperationResult]]
ApprovalFn = Callable[["ApprovalRequest"], Awaitable[None]]


@dataclass
class ApprovalRequest:
    workflow_id: str
    operation_name: str
    timeout: int
    approval_id: str
    justification: str | None = None


# ── Worker ───────────────────────────────────────────────────────────────────


# Worker endpoint resolution order: explicit arg -> env -> self-host default.
DEFAULT_WORKER_ADDRESS = "http://localhost:50052"


class BoundFlowWorker:
    # address keeps its leading position so existing positional calls still work;
    # to rely on the default/env, pass the client by keyword: BoundFlowWorker(llm=...).
    def __init__(self, address: str | None = None, llm: LlmClient | None = None,
                 api_key: str | None = None, trace_sink: TraceSink | None = None) -> None:
        import os
        if llm is None:
            raise ValueError("an LlmClient must be provided (e.g. BoundFlowWorker(llm=...))")
        key = api_key or os.environ.get("BOUNDFLOW_API_KEY") or ""
        if not key:
            raise ValueError("api_key must be provided or BOUNDFLOW_API_KEY must be set")
        self._address = address or os.environ.get("BOUNDFLOW_WORKER_ADDRESS") or DEFAULT_WORKER_ADDRESS
        self._api_key = key
        self._orchestrator = Orchestrator(llm)
        self._trace_sink = trace_sink
        self._workflows: dict[tuple[str, int], HandlerFn] = {}
        self._operations: dict[tuple[str, str], HandlerFn] = {}
        self._on_approval: ApprovalFn | None = None

    def workflow(self, type: str, *, version: int) -> Callable[[HandlerFn], HandlerFn]:
        """Register the entry handler for a workflow type + version."""

        def deco(fn: HandlerFn) -> HandlerFn:
            self._workflows[(type, version)] = fn
            return fn

        return deco

    def operation(self, type: str, name: str) -> Callable[[HandlerFn], HandlerFn]:
        """Register a named follow-on operation (e.g. an approval branch target)."""

        def deco(fn: HandlerFn) -> HandlerFn:
            self._operations[(type, name)] = fn
            return fn

        return deco

    def on_approval_requested(self, fn: ApprovalFn) -> ApprovalFn:
        self._on_approval = fn
        return fn

    async def run(self) -> None:
        """Open the worker stream and dispatch jobs until cancelled."""
        from . import _transport as t
        from boundflow.v1 import operation_pb2 as op_pb

        ENTRY = "invoke_entry"

        async def dispatch(op):  # op: AtomicOperation proto
            rtype = op.workflow_type
            if op.name == ENTRY:
                handler = self._workflows.get((rtype, op.workflow_version))
            else:
                handler = self._operations.get((rtype, op.name))
            if handler is None:
                raise RuntimeError(
                    f"No handler for workflow '{rtype}' operation '{op.name}' v{op.workflow_version}")

            ctx = OperationContext(_Operation(op), self._orchestrator, self._trace_sink)
            _op_start = now_ms()
            uncaught_reason: str | None = None
            try:
                result = await handler(ctx)
            except PlatformError:
                # Not a customer-domain failure: let it propagate so the transport reports
                # the operation as failed, interrupting the workflow instead of completing
                # the run and keeping it active.
                log.exception("workflow raised a platform error; interrupting the run (op_id=%s op=%s)", op.id, op.name)
                raise
            except Exception as ex:  # noqa: BLE001 — a crash in customer callback code is a
                # customer-domain failure (bumps num_failures for lifecycle policy), not a
                # platform failure. The run still completes so the workflow stays active.
                log.exception("workflow callback raised; recording as a failed run (op_id=%s op=%s)", op.id, op.name)
                ctx.mark_failed()
                result = Complete()
                uncaught_reason = f"{type(ex).__name__}: {ex}"
            _op_end = now_ms()

            # Mint the approval id once when the gate opens, so the trace's correlation
            # id matches the one sent to the server (and recorded in the audit log).
            approval_id = t.new_approval_id() if isinstance(result, AwaitApproval) else None

            if self._trace_sink is not None:
                await self._emit_operation_trace(op, ctx, result, _op_start, _op_end, approval_id)

            proto = await self._to_proto(result, op, approval_id)
            for name, snap in ctx.agent_state_updates.items():
                proto.agent_state_updates[name].CopyFrom(t.metrics_to_proto(snap))
            for name, action in ctx.agent_policy_actions.items():
                proto.agent_policy_actions[name].CopyFrom(t.agent_policy_action_to_proto(action))
            if ctx.failed:
                proto.workflow_metrics.CopyFrom(op_pb.WorkflowInvocationMetrics(failures=1))
                # Tag the soft failure so the server classifies the run outcome without
                # inferring: an exception carries its text; mark_failed() carries none.
                if uncaught_reason is not None:
                    proto.failure_type = op_pb.OPERATION_FAILURE_TYPE_UNCAUGHT_EXCEPTION
                    proto.failure_reason = uncaught_reason
                else:
                    proto.failure_type = op_pb.OPERATION_FAILURE_TYPE_CUSTOMER_MARKED
            return proto

        capabilities = list(self._workflows.keys())
        await t.WorkerSession(self._address, self._api_key, capabilities).run(dispatch)

    async def _emit_operation_trace(self, op, ctx, result, start_ms: int, end_ms: int, approval_id: str | None = None) -> None:
        """Build the operation trace (its agent runs + outcome) and hand it to the
        sink. Tracing is best-effort: a sink failure is logged and dropped, never
        fatal to the run. All operations of one invocation share trace_id (= op.id).
        When the operation parks for approval, approval_id is the key to correlate
        this trace with the server-side approval audit (GetApprovalAudit)."""
        outcome = (OUTCOME_AWAIT_APPROVAL if isinstance(result, AwaitApproval)
                   else OUTCOME_NEXT if isinstance(result, Next)
                   else OUTCOME_COMPLETED)
        try:
            await self._trace_sink.emit(OperationTrace(
                trace_id=op.id,
                workflow_id=op.workflow_id,
                workflow_type=op.workflow_type,
                version=op.workflow_version,
                operation=op.name,
                outcome=outcome,
                failed=ctx.failed,
                start_ms=start_ms,
                end_ms=end_ms,
                agent_runs=ctx._agent_runs,
                approval_id=approval_id,
            ))
        except Exception:  # noqa: BLE001 — tracing is best-effort, never fatal
            log.exception("trace sink emit failed; dropping operation trace %s", op.name)

    async def _to_proto(self, result: OperationResult, op, approval_id: str | None = None):
        """Map an OperationResult to an AtomicOperationResult proto. approval_id, when
        the result is an AwaitApproval, is the id minted by the caller (shared with
        the trace) rather than minted here."""
        from . import _transport as t
        from boundflow.v1 import operation_pb2 as op_pb

        completed = op_pb.OPERATION_STATUS_COMPLETED

        # The context handed to the next operation is the same bag we received (system
        # keys and all) with the customer's data slotted back into "input" — so the
        # runtime's keys keep flowing and ctx.context stays customer-only on the far side.
        current = t.context_to_dict(op)

        def carry(customer_context: dict):
            bag = dict(current)
            bag["input"] = customer_context or {}
            return t.dict_to_struct(bag)

        def branch(r: OperationResult):
            # A Next branch becomes an AtomicOperation; Complete becomes None.
            if isinstance(r, Next):
                return op_pb.AtomicOperation(
                    name=r.operation, timeout_seconds=r.timeout, context=carry(r.context))
            return None

        if isinstance(result, Complete):
            proto_result = t.dict_to_struct(result.result) if result.result is not None else None
            return op_pb.AtomicOperationResult(status=completed, result=proto_result)

        if isinstance(result, Next):
            return op_pb.AtomicOperationResult(
                status=completed,
                next_operation=op_pb.AtomicOperation(
                    name=result.operation, timeout_seconds=result.timeout,
                    context=carry(result.context)))

        if isinstance(result, AwaitApproval):
            if approval_id is None:
                approval_id = t.new_approval_id()
            if self._on_approval is not None:
                await self._on_approval(ApprovalRequest(
                    workflow_id=op.workflow_id, operation_name=op.name,
                    timeout=result.timeout, approval_id=approval_id,
                    justification=result.justification))
            gate = op_pb.ApprovalGate(timeout_seconds=result.timeout, approval_id=approval_id)
            ap = branch(result.on_approve)
            rj = branch(result.on_reject)
            if ap is not None:
                gate.on_approve.CopyFrom(ap)
            elif isinstance(result.on_approve, Complete) and result.on_approve.result is not None:
                gate.on_approve_result.CopyFrom(t.dict_to_struct(result.on_approve.result))
            if rj is not None:
                gate.on_reject.CopyFrom(rj)
            elif isinstance(result.on_reject, Complete) and result.on_reject.result is not None:
                gate.on_reject_result.CopyFrom(t.dict_to_struct(result.on_reject.result))
            return op_pb.AtomicOperationResult(status=completed, approval_gate=gate)

        raise RuntimeError(f"Unknown OperationResult: {type(result).__name__}")


class _Operation:
    """Adapter wrapping the AtomicOperation proto for OperationContext."""

    def __init__(self, op) -> None:
        from . import _transport as t
        self.name = op.name
        self.workflow_version = op.workflow_version
        self.context = t.context_to_dict(op)
        # Identifiers for the run trace (op.id is the request/invocation id = trace_id).
        self.request_id = op.id
        self.workflow_id = op.workflow_id
        self.workflow_type = op.workflow_type
