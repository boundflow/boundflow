"""Worker-side surface: agent definitions, tools, operation handlers.

Decorators for registration; plain async functions for tools and handlers.
"""

from __future__ import annotations

import inspect
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Union

from .errors import PlatformError
from .governed import AgentGovernor
from .lifecycle import (
    apply_lifecycle_rules,
    load_history,
    load_lifecycle_rules,
    load_runtime_policy,
)
from .llm import AgentPolicyLimitExceeded, AgentStepConfig, LlmClient, Orchestrator, StepResult
from .policies import RuntimePolicy, ToolCallLimit, ToolFailureLimit
from .trace import (
    OUTCOME_AWAIT_APPROVAL,
    OUTCOME_AWAIT_INPUT,
    OUTCOME_COMPLETED,
    OUTCOME_NEXT,
    AgentRunTrace,
    OperationTrace,
    TraceSink,
    now_ms,
)

log = logging.getLogger("boundflow.worker")

# The operation name the entry handler (@worker.workflow) is dispatched under.
# Reserved — an @worker.operation registered under this name would collide with
# entry dispatch. Pass this to Next(operation=...) to loop back to the start.
ENTRY_OPERATION = "invoke_entry"

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
    delay_seconds: int = 0


def _reject_delay_on_branch(branch: "OperationResult") -> None:
    if isinstance(branch, Next) and branch.delay_seconds:
        raise ValueError(
            "Next.delay_seconds is not supported on approval/input gate branches "
            "(on_approve/on_reject/on_answer/on_timeout); use it on a plain Next "
            "returned directly from an operation instead.")


@dataclass
class AwaitApproval:
    """Park for human approval; branch on the decision. `justification` and
    `metadata` are published for external readers while the gate is open
    (WorkflowInfo.pending_approval, via get_workflow)."""

    on_approve: "OperationResult"
    on_reject: "OperationResult"
    timeout: int
    justification: str | None = None
    metadata: dict | None = None

    def __post_init__(self):
        _reject_delay_on_branch(self.on_approve)
        _reject_delay_on_branch(self.on_reject)


@dataclass
class AwaitInput:
    """Park for a free-form answer, not a binary decision. `on_answer` resumes with
    the answer readable via `ctx.input_answer`; `on_timeout` runs if nobody answers.
    `prompt` and `metadata` are published for external readers while the gate is open
    (WorkflowInfo.pending_input, via get_workflow)."""

    on_answer: "OperationResult"
    on_timeout: "OperationResult"
    timeout: int
    prompt: str | None = None
    metadata: dict | None = None

    def __post_init__(self):
        _reject_delay_on_branch(self.on_answer)
        _reject_delay_on_branch(self.on_timeout)


OperationResult = Union[Complete, Next, AwaitApproval, AwaitInput]


@dataclass
class Budget:
    """What's left to spend on one `run_agent` call, for budgets spanning several
    steps — RuntimePolicy caps a single step, so a loop gets a fresh cap each time.

        await ctx.run_agent(agent, budget=Budget(max_cost_usd=TOTAL - spent))

    Only ever narrows what policy allows: this is called from workflow code, so
    policy stays the ceiling. Fields are None (no constraint) / <= 0 (spent) /
    > 0 (cap at min(policy, this)). Unlike RuntimePolicy, 0 here isn't "unset".
    """

    max_llm_calls: int | None = None
    max_cost_usd: float | None = None
    # Remaining per tool. These don't raise when spent — one exhausted tool doesn't
    # stop the step, it just gets blocked (or, for failures, ends the run when it
    # next fails).
    tool_call_limits: dict[str, int] | None = None
    tool_failure_limits: dict[str, int] | None = None


def _apply_budget(policy: RuntimePolicy, budget: Budget | None, agent_name: str) -> RuntimePolicy:
    """Tighten `policy` with `budget`. Never loosens: workflow code can spend less
    than policy allows, never more."""
    if budget is None:
        return policy

    for label, remaining in (("max_llm_calls", budget.max_llm_calls),
                             ("max_cost_usd", budget.max_cost_usd)):
        # 0 means "unlimited" in RuntimePolicy, so a spent budget must refuse here
        # rather than pass through and remove the cap.
        if remaining is not None and remaining <= 0:
            raise AgentPolicyLimitExceeded(
                f"agent {agent_name!r} has no {label} budget left "
                f"(remaining: {remaining}); not making a call")

    tightened = policy.model_copy()
    if budget.max_llm_calls is not None:
        tightened.max_llm_calls = (min(policy.max_llm_calls, budget.max_llm_calls)
                                   if policy.max_llm_calls > 0 else budget.max_llm_calls)
    if budget.max_cost_usd is not None:
        tightened.max_cost_usd = (min(policy.max_cost_usd, budget.max_cost_usd)
                                  if policy.max_cost_usd > 0 else budget.max_cost_usd)

    if budget.tool_call_limits is not None:
        tightened.tool_call_limits = [
            ToolCallLimit(tool=tool, max_calls=max(0, remaining))
            for tool, remaining in _tighten_per_tool(
                {l.tool: l.max_calls for l in policy.tool_call_limits},
                budget.tool_call_limits).items()
        ]
    if budget.tool_failure_limits is not None:
        tightened.tool_failure_limits = [
            ToolFailureLimit(tool=tool, max_failures=max(0, remaining))
            for tool, remaining in _tighten_per_tool(
                {l.tool: l.max_failures for l in policy.tool_failure_limits},
                budget.tool_failure_limits).items()
        ]
    return tightened


def _tighten_per_tool(policy_caps: dict[str, int], remaining: dict[str, int]) -> dict[str, int]:
    """Per tool: the smaller of policy and remaining. Never loosens."""
    out = dict(policy_caps)
    for tool, left in remaining.items():
        existing = out.get(tool)
        out[tool] = left if existing is None else min(existing, left)
    return out


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
        # Governors for customer-driven agent loops (ctx.agent_model), keyed by agent
        # name. Flushed into agent_state_updates/_agent_runs when the operation ends,
        # since a governed loop has no completion point of its own.
        self._governors: dict[str, AgentGovernor] = {}

    @property
    def name(self) -> str:
        return self._op.name

    @property
    def workflow_version(self) -> int:
        return self._op.workflow_version

    @property
    def context(self) -> dict:
        """The operation's context — the caller's own keys, read and written freely
        (seeded by invoke_workflow(context=...) and carried across operations)."""
        raw = self._op.context
        if not isinstance(raw.get("input"), dict):
            raw["input"] = {}
        return raw["input"]

    @property
    def input_answer(self) -> Any:
        """The answer submitted via submit_input(), when this operation was reached
        through an AwaitInput on_answer branch; None otherwise."""
        return self.context.pop("answer", None)

    @property
    def approval_reason(self) -> str | None:
        """The reason given to approve_workflow()/reject_workflow(), when this
        operation was reached through an AwaitApproval branch; None if the decider
        didn't give one (or on a timeout, which has no decider).

        The gate's own justification isn't here — you wrote that when building the
        gate, so thread it through the branch's context if the operation needs it.
        The reason comes from outside the workflow, so only the server can supply it."""
        return self.context.pop("approval_reason", None)

    def add_context(self, metadata: str, payload: Any, *, key: str | None = None) -> "OperationContext":
        self._llm_context.append((key or metadata, metadata, payload))
        return self

    def mark_failed(self) -> None:
        """Flag this run as a customer-side failure (increments num_failures)."""
        self.failed = True

    def _resolve_policy(self, agent_name: str) -> RuntimePolicy:
        """The agent's effective runtime policy for this operation. Runtime policy is
        snapshotted at request-creation time; lifecycle policy + metrics history are
        injected by the scheduler. Lifecycle rules are evaluated here and may change
        the effective policy — when they do, the change is queued for server-side audit."""
        runtime_node = (self._op.context.get("agentRuntimePolicies") or {}).get(agent_name)
        state_node = (self._op.context.get("agentStates") or {}).get(agent_name)

        base_policy = load_runtime_policy(runtime_node)
        rules = load_lifecycle_rules(state_node)
        history = load_history(state_node)

        runtime_policy, fired = apply_lifecycle_rules(rules, history, base_policy)

        # Audit the firing only when it actually changed the policy (effective != base).
        if fired and runtime_policy != base_policy:
            self.agent_policy_actions[agent_name] = {
                "base_policy": base_policy,
                "effective_policy": runtime_policy,
                "fired_rules": fired,
            }
        return runtime_policy

    async def run_agent(self, agent: AgentDefinition, *, budget: Budget | None = None) -> StepResult:
        """Run an agent step — BoundFlow drives the loop. Metrics are written back on
        completion. See `agent_model()` for the inverse (you drive, BoundFlow governs).

        `budget` narrows this step to what's left of a longer budget (see `Budget`);
        applied after lifecycle rules, and only ever tightens."""
        runtime_policy = _apply_budget(self._resolve_policy(agent.name), budget, agent.name)
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
            "latency_seconds": (_run_end - _run_start) / 1000.0,
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

    def agent_governor(self, agent_name: str, *, model: str = "",
                       budget: Budget | None = None) -> AgentGovernor:
        """The agent's governor — the framework-agnostic half of `agent_model()`.

        Use this to govern a framework BoundFlow has no adapter for (CrewAI's
        `BaseLLM`, a raw provider client, a hand-rolled loop): call
        `governor.begin_call()` before each model call and `call.record(usage, ...)`
        after. See `boundflow.governed` for the full adapter contract.

        Repeated calls for the same agent return the same governor, so caps and
        metrics accumulate across every call the loop makes this operation."""
        existing = self._governors.get(agent_name)
        if existing is not None:
            # agent_tools() can create the governor before agent_model() supplies a
            # model, so fill it in late rather than depending on call order.
            if model and not existing.model:
                existing.model = existing.policy.model or model
            return existing
        governor = AgentGovernor(
            agent_name=agent_name,
            policy=_apply_budget(self._resolve_policy(agent_name), budget, agent_name),
            default_model=model,
            pricing=(self._op.context.get("modelPricing") or {}),
            collect_spans=self._sink is not None,
        )
        self._governors[agent_name] = governor
        return governor

    def agent_model(self, agent_name: str, chat_model: Any, *, model: str | None = None,
                    budget: Budget | None = None) -> Any:
        """A governed LangChain chat model — the inverse of `run_agent()`. You drive
        the loop (LangGraph, a chain, anything taking a `BaseChatModel`); BoundFlow
        governs each call under `agent_name`'s runtime policy and records its cost,
        tokens, and spans against this operation.

            model = ctx.agent_model("responder", ChatAnthropic(model=HAIKU))
            graph = build_graph(model)          # LangGraph owns messages/loops/memory
            await graph.ainvoke({"messages": [...]})

        `chat_model` is a `BaseChatModel`, or a callable `(model_name) -> BaseChatModel`
        — pass a factory if you want `SetModel` lifecycle policies to take effect,
        since only a factory can build the policy-chosen model.

        `model` is the model id used for pricing and as the `SetModel` default;
        derived from `chat_model` when omitted, and required for a factory.

        Note: to have `tool_call_limits` enforced too, pass your tools through
        `agent_tools()` — BoundFlow can only stop a tool it dispatches."""
        from .langchain_client import GovernedChatModel

        is_factory = callable(chat_model) and not hasattr(chat_model, "ainvoke")
        if model is None:
            if is_factory:
                raise ValueError(
                    "agent_model(model=...) is required when passing a factory — "
                    "BoundFlow needs the model id to price calls and to build the "
                    "default model.")
            model = _derive_model_name(chat_model)

        governor = self.agent_governor(agent_name, model=model, budget=budget)
        return GovernedChatModel(governor=governor, chat_model=chat_model)

    def agent_tools(self, agent_name: str, tools: list, *, budget: Budget | None = None,
                    output_schema: dict | None = None) -> list:
        """Governed LangChain tools — hand BoundFlow the tools you want governed, the
        way `agent_model()` hands it the model you want governed.

            model = ctx.agent_model("researcher", ChatAnthropic(model=MODEL))
            tools = ctx.agent_tools("researcher", [search, calculator])
            agent = create_react_agent(model, tools)

        BoundFlow then dispatches these tools, which is what makes `tool_call_limits`
        enforceable: a tool whose cap is spent isn't run, and the model is told so —
        the same refusal `run_agent` returns, so it adapts instead of failing. Tool
        failures and per-tool spans get recorded too, which they can't be for tools
        BoundFlow never sees.

        Wrappers keep the original name, description, and args schema, so the model
        sees exactly the tools you defined."""
        from .langchain_client import governed_tools

        return governed_tools(self.agent_governor(agent_name, budget=budget), tools,
                              output_schema=output_schema)

    async def run_governed(self, agent_name: str, invoke, *, chat_model: Any,
                           tools: list | None = None, model: str | None = None,
                           output_schema: dict | None = None,
                           budget: Budget | None = None) -> StepResult:
        """Run someone else's agent harness under governance and get a StepResult back.

        `invoke(model, tools)` builds and runs the harness. **Its return value is the
        deliverable** — a harness completing normally ends the way it always does, and
        we hand back what it produced:

            result = await ctx.run_governed(
                "researcher",
                lambda m, t: create_deep_agent(model=m, tools=t).ainvoke({"messages": [...]}),
                chat_model=ChatAnthropic(model=MODEL),
            )
            result.output   # whatever the harness returned

        Want a typed deliverable? Declare it the harness's way, not ours —
        `create_deep_agent(response_format=MyModel)` puts a parsed object in
        `structured_response`. Injecting our own terminator would change what the agent
        does, which is the wrong side of the seam: a caller using the harness directly
        must get the same behaviour.

        `output_schema` is **only** the cap-exhaustion escape hatch. It injects a
        submit_result tool so a run that spends its budget mid-flight ends gracefully
        with whatever it had, instead of raising and losing the work. It is not the
        completion path, and a harness that finishes normally never touches it."""
        governor = self.agent_governor(agent_name, model=model or "", budget=budget)
        governed = self.agent_model(agent_name, chat_model, model=model, budget=budget)
        governed_tool_list = self.agent_tools(
            agent_name, tools or [], budget=budget, output_schema=output_schema)

        from .governed import AgentFinalized
        try:
            output = await invoke(governed, governed_tool_list)
        except AgentFinalized as finished:
            # A cap was spent mid-run and the injected terminator fired instead of
            # raising, so the partial deliverable survives.
            output = finished.output

        return StepResult(
            output, governor.llm_calls, governor.cost_usd, governor.tokens_used,
            dict(governor.calls_per_tool), dict(governor.tool_failure_counts),
            governor.model, governor.spans)

    def _flush_governors(self) -> None:
        """Fold governed-loop metrics into the operation result. Called once the
        handler returns, since a customer-driven loop has no completion point of
        its own for us to hook."""
        for name, governor in self._governors.items():
            governor.warn_if_tool_limits_unenforced()
            if governor.llm_calls == 0:
                continue  # a model that was never called shouldn't emit a run
            self.agent_state_updates[name] = governor.snapshot()
            if self._sink is not None:
                self._agent_runs.append(governor.trace())


def _derive_model_name(chat_model: Any) -> str:
    """LangChain chat models carry their id on `.model_name` or `.model`, depending
    on the provider."""
    for attr in ("model_name", "model", "model_id"):
        value = getattr(chat_model, attr, None)
        if isinstance(value, str) and value:
            return value
    raise ValueError(
        f"could not derive a model id from {type(chat_model).__name__!r}; "
        "pass it explicitly as agent_model(..., model='...') so BoundFlow can "
        "price calls against it.")


HandlerFn = Callable[[OperationContext], Awaitable[OperationResult]]
ApprovalFn = Callable[["ApprovalRequest"], Awaitable[None]]
InputFn = Callable[["InputRequest"], Awaitable[None]]


@dataclass
class ApprovalRequest:
    workflow_id: str
    operation_name: str
    timeout: int
    approval_id: str
    justification: str | None = None
    metadata: dict | None = None


@dataclass
class InputRequest:
    workflow_id: str
    operation_name: str
    timeout: int
    input_id: str
    prompt: str | None = None
    metadata: dict | None = None


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
        self._on_input: InputFn | None = None

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

    def on_input_requested(self, fn: InputFn) -> InputFn:
        self._on_input = fn
        return fn

    async def run(self) -> None:
        """Open the worker stream and dispatch jobs until cancelled."""
        from . import _transport as t
        from boundflow.v1 import operation_pb2 as op_pb

        async def dispatch(op):  # op: AtomicOperation proto
            rtype = op.workflow_type
            if op.name == ENTRY_OPERATION:
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
            # After the handler either way (including the failure path) — a run that
            # blew a cap still spent real money, and the receipt has to say so.
            ctx._flush_governors()
            _op_end = now_ms()

            # Mint the approval/input id once when the gate opens, so the trace's
            # correlation id matches the one sent to the server (and recorded in the
            # audit log).
            approval_id = t.new_approval_id() if isinstance(result, AwaitApproval) else None
            input_id = t.new_input_id() if isinstance(result, AwaitInput) else None

            if self._trace_sink is not None:
                await self._emit_operation_trace(op, ctx, result, _op_start, _op_end, approval_id, input_id)

            proto = await self._to_proto(result, op, approval_id, input_id)
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

    async def _emit_operation_trace(self, op, ctx, result, start_ms: int, end_ms: int,
                                     approval_id: str | None = None, input_id: str | None = None) -> None:
        """Build the operation trace (its agent runs + outcome) and hand it to the
        sink. Tracing is best-effort: a sink failure is logged and dropped, never
        fatal to the run. All operations of one invocation share trace_id (= op.id).
        When the operation parks for approval/input, approval_id/input_id is the key
        to correlate this trace with the server-side audit (GetApprovalAudit /
        GetInputAudit)."""
        outcome = (OUTCOME_AWAIT_APPROVAL if isinstance(result, AwaitApproval)
                   else OUTCOME_AWAIT_INPUT if isinstance(result, AwaitInput)
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
                input_id=input_id,
            ))
        except Exception:  # noqa: BLE001 — tracing is best-effort, never fatal
            log.exception("trace sink emit failed; dropping operation trace %s", op.name)

    async def _to_proto(self, result: OperationResult, op, approval_id: str | None = None,
                         input_id: str | None = None):
        """Map an OperationResult to an AtomicOperationResult proto. approval_id/input_id,
        when the result is an AwaitApproval/AwaitInput, is the id minted by the caller
        (shared with the trace) rather than minted here."""
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
            # delay_seconds is rejected at AwaitApproval/AwaitInput construction
            # time, so r.delay_seconds is always 0 here.
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
                    context=carry(result.context), delay_seconds=result.delay_seconds))

        if isinstance(result, AwaitApproval):
            if approval_id is None:
                approval_id = t.new_approval_id()
            if self._on_approval is not None:
                await self._on_approval(ApprovalRequest(
                    workflow_id=op.workflow_id, operation_name=op.name,
                    timeout=result.timeout, approval_id=approval_id,
                    justification=result.justification, metadata=result.metadata))
            gate = op_pb.ApprovalGate(
                timeout_seconds=result.timeout, approval_id=approval_id,
                justification=result.justification or "")
            if result.metadata is not None:
                gate.metadata.CopyFrom(t.dict_to_struct(result.metadata))
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

        if isinstance(result, AwaitInput):
            if input_id is None:
                input_id = t.new_input_id()
            if self._on_input is not None:
                await self._on_input(InputRequest(
                    workflow_id=op.workflow_id, operation_name=op.name,
                    timeout=result.timeout, input_id=input_id,
                    prompt=result.prompt, metadata=result.metadata))
            gate = op_pb.InputGate(
                timeout_seconds=result.timeout, input_id=input_id,
                prompt=result.prompt or "")
            if result.metadata is not None:
                gate.metadata.CopyFrom(t.dict_to_struct(result.metadata))
            ans = branch(result.on_answer)
            to = branch(result.on_timeout)
            if ans is not None:
                gate.on_answer.CopyFrom(ans)
            elif isinstance(result.on_answer, Complete) and result.on_answer.result is not None:
                gate.on_answer_result.CopyFrom(t.dict_to_struct(result.on_answer.result))
            if to is not None:
                gate.on_timeout.CopyFrom(to)
            elif isinstance(result.on_timeout, Complete) and result.on_timeout.result is not None:
                gate.on_timeout_result.CopyFrom(t.dict_to_struct(result.on_timeout.result))
            return op_pb.AtomicOperationResult(status=completed, input_gate=gate)

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
