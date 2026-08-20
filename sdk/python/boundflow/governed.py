"""Governed model access for agent loops BoundFlow doesn't drive.

`ctx.run_agent()` has BoundFlow own the agent loop. This is the inverse: the
customer owns the loop (LangGraph, CrewAI, a hand-rolled one) and BoundFlow
governs the individual model calls — caps, cost metering, per-agent metrics and
trace spans — without dictating how the agent is structured.

`AgentGovernor` is deliberately framework-agnostic: it speaks only BoundFlow
types (`RuntimePolicy`, `Usage`, `Span`) and imports nothing from any agent
framework. Per-framework adapters wrap it in whatever shape that framework
expects — see `GovernedChatModel` in `langchain_client.py` for the LangChain /
LangGraph one (CrewAI's `BaseLLM.call()` would be a sibling).

Adapter contract, once per model call:

    call = governor.begin_call()        # enforces caps; raises if already over
    # ...invoke the underlying model, honouring call.model / call.max_tokens
    #    / call.timeout_seconds...
    call.record(usage, tool_calls=[...], input_messages=[...], output_message={...})

`begin_call()` returning the effective model is what makes the contract hard to
misuse: an adapter can't skip the cap check and still know which model to call.

## What this layer can and can't enforce

Owning the loop lets `run_agent` degrade gracefully (force `submit_result` on
the last allowed call). Governing individual calls can only hard-stop — raise,
and let the failure propagate to the workflow handler. So:

- `model` (incl. `SetModel`), `max_tokens_per_call`, `max_call_seconds`: enforced
- `max_llm_calls`, `max_cost_usd`: enforced, but as a hard stop, and `max_cost_usd`
  is checked against spend already recorded, so the call that crosses the line
  still completes (same post-hoc limitation `run_agent` has today)
- `tool_call_limits`: enforced **only for tools passed through `ctx.agent_tools()`**,
  since BoundFlow can only stop a tool it dispatches. Hand it the tools and a spent
  cap blocks execution and tells the model, exactly as `run_agent` does; keep the
  tools to yourself and the cap can't apply — tool calls are still *observed* from
  the model's requests (so `calls_per_tool` keeps feeding lifecycle rules), but
  nothing is blocked, and the caller is warned at the end of the operation.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from .llm import (
    AgentPolicyLimitExceeded,
    ToolFailureLimitExceeded,
    Usage,
    _estimate_cost,
    _gen_ai_system,
    tool_limit_message,
)
from .policies import RuntimePolicy
from .trace import (
    BF_COST_USD,
    GEN_AI_OP_CHAT,
    GEN_AI_OP_EXECUTE_TOOL,
    GEN_AI_OPERATION_NAME,
    GEN_AI_TOOL_CALL_ID,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    SPAN_KIND_TOOL,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_SYSTEM,
    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS,
    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    ROLE_ASSISTANT,
    SPAN_KIND_LLM,
    Span,
    now_ms,
)

log = logging.getLogger("boundflow.governed")

DEFAULT_MAX_TOKENS = 4096

# Cost is only known after a call returns, so a concurrent batch can't be checked
# against the cap the way call count can. Stopping slightly early leaves room for
# whatever is in flight to land inside the declared budget.
COST_HEADROOM = 0.9


class AgentFinalized(Exception):
    """Raised by the injected `submit_result` tool to end a governed harness with a
    structured answer.

    BoundFlow's own loop can just return when the model submits; someone else's
    loop can't be returned from, so the only way out is to raise. `ctx.run_governed`
    catches this and hands back a StepResult, so callers don't see the exception."""

    def __init__(self, output: dict) -> None:
        self.output = output
        super().__init__("agent submitted its final result")


@dataclass
class GovernedCall:
    """One permitted model call. Returned by `AgentGovernor.begin_call()` with the
    policy-resolved parameters the adapter must honour, and closed by `record()`."""

    model: str
    max_tokens: int
    timeout_seconds: float  # 0 = unset (no per-call timeout)
    # True when this is the last call the policy allows. How a harness expresses
    # that is its own business: BoundFlow's loop forces submit_result, a governed
    # harness forces its injected equivalent. Either way the agent gets one real
    # chance to finish instead of being cut off.
    finalize: bool = False
    _governor: "AgentGovernor" = None  # type: ignore[assignment]
    _start_ms: int = field(default_factory=now_ms)
    _recorded: bool = False

    def abandon(self) -> None:
        """Release this call's reservation — it never reached the model.

        Without this a provider having a bad day drains the budget with calls that
        were never billed, and the agent gets throttled for work it never did. A
        no-op after `record()`, which earned the slot.
        """
        if self._recorded:
            return
        self._recorded = True
        self._governor.llm_calls -= 1

    async def __aenter__(self) -> "GovernedCall":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        # Releases only when record() never ran, so an adapter has no exception path
        # to remember and a leak can't be introduced by forgetting one.
        self.abandon()

    def record(
        self,
        usage: Usage,
        *,
        tool_calls: list[str] | None = None,
        input_messages: list | None = None,
        output_message: dict | None = None,
        extra_attributes: dict | None = None,
    ) -> float:
        """Close the call: accumulate cost/tokens, count any tool calls the model
        asked for, and emit the LLM span. Returns this call's cost in USD.

        `input_messages` / `output_message` are canonical GenAI-shaped messages
        (the adapter converts from its framework's types); they're only used for
        the trace, so passing None just yields a span without content.
        """
        if self._recorded:
            raise RuntimeError("GovernedCall.record() called twice for one call")
        self._recorded = True
        return self._governor._record(
            self, usage, tool_calls or [], input_messages, output_message,
            extra_attributes or {})


@dataclass
class GovernedToolCall:
    """One tool invocation. `denied` means the per-tool cap is spent and the tool
    must NOT run — hand `denial_message` back to the model as the tool's result,
    the same refusal `run_agent` returns."""

    tool: str
    denied: bool
    denial_message: str
    call_id: str = ""
    description: str = ""
    _governor: "AgentGovernor" = None  # type: ignore[assignment]
    _start_ms: int = field(default_factory=now_ms)
    _recorded: bool = False

    def record(self, *, input: Any = None, output: Any = None, error: BaseException | None = None) -> None:
        """Close the tool call: count a failure if it raised, and emit the tool span."""
        if self._recorded or self.denied:
            return
        self._recorded = True
        self._governor._record_tool(self, input, output, error)


class AgentGovernor:
    """Per-agent governance for a customer-driven loop: enforces the agent's
    runtime policy across the model calls the loop makes, and accumulates the
    metrics/spans BoundFlow reports for the operation.

    One governor per (operation, agent name). Not thread-safe; it assumes the
    single-threaded async model the SDK already uses.
    """

    def __init__(
        self,
        agent_name: str,
        policy: RuntimePolicy,
        default_model: str,
        pricing: dict | None = None,
        *,
        collect_spans: bool = True,
        can_finalize: bool = False,
    ) -> None:
        self.agent_name = agent_name
        self.policy = policy
        # A SetModel lifecycle action overrides the caller's default.
        self.model = policy.model or default_model
        self._pricing = pricing or {}
        self._collect_spans = collect_spans
        # Whether this harness can act on a finalize call (has a submit_result-shaped
        # terminator). Without one, a spent cap has to raise.
        self.can_finalize = can_finalize
        # Set by agent_tools(output_schema=...) once a submit_result tool exists.
        self.finalize_tool: str | None = None
        self.final_output: dict | None = None

        self.llm_calls = 0
        self.cost_usd = 0.0
        self.tokens_used = 0
        self.calls_per_tool: dict[str, int] = {}
        self.tool_failure_counts: dict[str, int] = {}
        self.spans: list[Span] = []
        self._start_ms = now_ms()

        # Set once a finalize call has been handed out, so the next begin_call()
        # raises rather than offering a second one.
        self._finalize_offered = False
        self._tool_limits = {l.tool: l.max_calls for l in policy.tool_call_limits}
        self._tool_failure_limits = {l.tool: l.max_failures for l in policy.tool_failure_limits}
        # Tools handed to us via agent_tools(): we dispatch these, so their caps are
        # enforceable and their calls are counted at execution rather than inferred
        # from what the model asked for.
        self._governed_tools: set[str] = set()
        self._harness_observer_active = False
        self._harness_metering_active = False
        # Model calls seen in the harness's own state. Distinct from `llm_calls`, which
        # counts reservations and so can't see a call that bypassed the governor.
        self.observed_llm_calls = 0

    def register_governed_tools(self, names: list[str]) -> None:
        self._governed_tools.update(names)

    def register_harness_metering(self) -> None:
        """Take the harness's own numbers as the truth about spend.

        Set by `MeteringSaver`, which reads usage off the messages the harness writes.
        Once it's on, our own per-call accumulation stands down — the same call would
        otherwise be counted twice, once when we make it and once when it's written —
        and the harness becomes the single source for tokens and cost. Enforcement is
        unaffected: reservations are still taken per call, they just no longer decide
        what gets reported."""
        self._harness_metering_active = True

    def record_harness_usage(self, *, input_tokens: int, output_tokens: int,
                             details: dict, model: str | None,
                             tool_calls: list[str] | None = None) -> None:
        """One model message the harness wrote, priced and accumulated.

        `model` is the one that actually served the call, which is not always
        `self.model` — a subagent may run a different one — so it prices per message
        rather than per agent.

        Tool calls are ignored here: these are the calls the model *asked* for, while
        the callbacks in `harness_callbacks` see the ones that ran and how they ended.
        Counting both would double them.
        """
        usage = Usage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=details.get("cache_creation", 0),
            cache_read_input_tokens=details.get("cache_read", 0),
        )
        self.cost_usd += _estimate_cost(usage, model or self.model, self._pricing)
        self.tokens_used += usage.total_tokens()
        # Counted separately from the reservation: this includes calls we never
        # reserved, like a subagent that built its own client and bypassed us entirely.
        self.observed_llm_calls += 1

    def register_harness_observer(self) -> None:
        """Hand metering of undeclared tools to the harness's own callbacks.

        Without them we count a harness's tools from the model's *request*, which is all
        we can see — it tells us a call happened, never how it ended. LangChain's tool
        callbacks fire around the execution itself, so once installed they count what
        actually ran and how it ended, and the request-side count stands down rather than
        double-counting the same call. See `harness_callbacks.py`."""
        self._harness_observer_active = True

    def is_governed_tool(self, tool: str) -> bool:
        return tool in self._governed_tools

    def tool_call_cap(self, tool: str) -> int | None:
        return self._tool_limits.get(tool)

    def tool_call_caps(self) -> dict[str, int]:
        """Every declared per-tool cap, for a harness that enforces its own.

        deepagents counts tool calls natively (`ToolCallLimitMiddleware`), so when one
        is running we hand it the policy rather than duplicating the counter — the
        policy is still ours, versioned and central; the enforcement is the harness's."""
        return dict(self._tool_limits)

    def capability_call_caps(self) -> dict[str, int]:
        """Per-capability caps — how many `write`s, not how many `write_file`s.

        No harness counts these (deepagents' limiter is one tool or all tools, nothing
        between), so unlike `tool_call_caps` these are enforced by BoundFlow middleware.
        See `capabilities.py`."""
        return {l.capability: l.max_calls for l in self.policy.capability_call_limits}

    def record_harness_tool(self, tool: str, *, failed: bool = False) -> None:
        """Record one harness-injected tool call that actually executed.

        Called *after* execution, by middleware that wrapped it — so unlike the
        request-side count this reflects what ran, and knows how it ended. Refused
        calls are never recorded: they didn't run, and counting them would push the
        number past its own cap in the metric lifecycle rules read.

        Raises `ToolFailureLimitExceeded` on a spent failure budget, matching what a
        declared tool does — a broken integration should trip its own circuit breaker
        rather than quietly burning the run's budget."""
        self.calls_per_tool[tool] = self.calls_per_tool.get(tool, 0) + 1
        if not failed:
            return
        failures = self.tool_failure_counts.get(tool, 0) + 1
        self.tool_failure_counts[tool] = failures
        cap = self._tool_failure_limits.get(tool)
        if cap is not None and failures >= cap:
            raise ToolFailureLimitExceeded(tool, failures, cap)

    def register_finalizer(self, tool: str) -> None:
        """A submit_result-shaped tool exists, so a spent cap can ask for a final
        answer instead of raising."""
        self.finalize_tool = tool
        self.can_finalize = True

    @property
    def max_tokens(self) -> int:
        return self.policy.max_tokens_per_call or DEFAULT_MAX_TOKENS

    def begin_call(self) -> GovernedCall:
        """Authorise one model call.

        Returns a call with `finalize=True` when this is the last one the policy
        allows, so the harness can ask for a final answer instead of being cut off.
        Raises `AgentPolicyLimitExceeded` once that chance has been used, or
        immediately when the harness can't express a finalize (`can_finalize=False`)
        — a customer-domain failure, so the operation completes marked failed.
        """
        # `llm_calls` counts calls *reserved*, not calls completed — the reservation
        # is taken below, before this returns. Concurrent callers (parallel subagents
        # share one governor) would otherwise all read the same pre-call count and all
        # pass: a cap of 1 admitted 5 simultaneous calls. Same discipline the harness
        # uses for tool caps, which it settles synchronously in `after_model` rather
        # than across the await.
        calls_spent = (self.policy.max_llm_calls > 0
                       and self.llm_calls >= self.policy.max_llm_calls)
        # Cost can't be reserved — it isn't known until the response lands — so it
        # keeps headroom instead: trip early enough that calls already in flight land
        # inside the declared budget rather than past it.
        cost_spent = (self.policy.max_cost_usd > 0
                      and self.cost_usd >= self.policy.max_cost_usd * COST_HEADROOM)

        if calls_spent or cost_spent:
            if self._finalize_offered or not self.can_finalize:
                which = "max_llm_calls" if calls_spent else "max_cost_usd"
                spent = (self.llm_calls if calls_spent else round(self.cost_usd, 4))
                cap = (self.policy.max_llm_calls if calls_spent else self.policy.max_cost_usd)
                raise AgentPolicyLimitExceeded(
                    f"agent {self.agent_name!r} reached {which}={cap} (used: {spent})")
            finalize = True
        else:
            # The last call the cap allows — offered as a finalize so the answer
            # lands cleanly rather than the next call raising.
            finalize = (self.can_finalize and self.policy.max_llm_calls > 0
                        and self.llm_calls == self.policy.max_llm_calls - 1)

        if finalize:
            self._finalize_offered = True
        self.llm_calls += 1  # reserved; released by abandon(), kept by record()
        return GovernedCall(
            model=self.model,
            max_tokens=self.max_tokens,
            timeout_seconds=self.policy.max_call_seconds,
            finalize=finalize,
            _governor=self,
        )

    def _record(
        self,
        call: GovernedCall,
        usage: Usage,
        tool_calls: list[str],
        input_messages: list | None,
        output_message: dict | None,
        extra_attributes: dict,
    ) -> float:
        end_ms = now_ms()
        cost = _estimate_cost(usage, self.model, self._pricing)

        # llm_calls was incremented at begin_call(); this call held that slot.
        if not self._harness_metering_active:
            # Otherwise the harness records this same call when it writes the message,
            # and its numbers are the ones that count.
            self.cost_usd += cost
            self.tokens_used += usage.total_tokens()
        for name in tool_calls:
            # A governed tool counts itself when it actually runs — counting the
            # model's *request* here too would double it, and would also count calls
            # that were denied by a cap or never dispatched.
            if name not in self._governed_tools and not self._harness_observer_active:
                self.calls_per_tool[name] = self.calls_per_tool.get(name, 0) + 1

        if self._collect_spans:
            self.spans.append(Span(
                kind=SPAN_KIND_LLM,
                name=f"{GEN_AI_OP_CHAT} {self.model}",
                start_ms=call._start_ms,
                end_ms=end_ms,
                input=input_messages,
                output=[output_message] if output_message else None,
                attributes={
                    GEN_AI_OPERATION_NAME: GEN_AI_OP_CHAT,
                    GEN_AI_SYSTEM: _gen_ai_system(self.model),
                    GEN_AI_REQUEST_MODEL: self.model,
                    GEN_AI_REQUEST_MAX_TOKENS: call.max_tokens,
                    GEN_AI_USAGE_INPUT_TOKENS: usage.input_tokens,
                    GEN_AI_USAGE_OUTPUT_TOKENS: usage.output_tokens,
                    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS: usage.cache_creation_input_tokens,
                    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS: usage.cache_read_input_tokens,
                    BF_COST_USD: cost,
                    **extra_attributes,
                },
            ))

        log.debug("governed call: agent=%s model=%s calls=%d cost=%.6f tokens=%d",
                  self.agent_name, self.model, self.llm_calls, self.cost_usd, self.tokens_used)
        return cost

    def begin_tool_call(self, tool: str, *, call_id: str = "",
                        description: str = "") -> GovernedToolCall:
        """Authorise one tool invocation against `tool_call_limits`.

        Unlike a model cap this doesn't raise: a spent per-tool cap is a signal to
        the *model*, not a failure of the run, so the caller returns
        `denial_message` as the tool's result and the model carries on without that
        tool — matching what `run_agent` does today."""
        cap = self._tool_limits.get(tool)
        used = self.calls_per_tool.get(tool, 0)
        if cap is not None and used >= cap:
            log.debug("tool_limit hit: agent=%s tool=%s count=%d cap=%d",
                      self.agent_name, tool, used, cap)
            return GovernedToolCall(tool=tool, denied=True,
                                    denial_message=tool_limit_message(tool, cap),
                                    call_id=call_id, description=description, _governor=self)
        self.calls_per_tool[tool] = used + 1
        return GovernedToolCall(tool=tool, denied=False, denial_message="",
                                call_id=call_id, description=description, _governor=self)

    def _record_tool(self, call: GovernedToolCall, input: Any,
                     output: Any, error: BaseException | None) -> None:
        failures = None
        if error is not None:
            failures = self.tool_failure_counts.get(call.tool, 0) + 1
            self.tool_failure_counts[call.tool] = failures
        if self._collect_spans:
            self.spans.append(Span(
                kind=SPAN_KIND_TOOL,
                name=call.tool,
                start_ms=call._start_ms,
                end_ms=now_ms(),
                input=input,
                output=None if error is not None else output,
                error=str(error) if error is not None else None,
                attributes={
                    GEN_AI_OPERATION_NAME: GEN_AI_OP_EXECUTE_TOOL,
                    GEN_AI_TOOL_NAME: call.tool,
                    GEN_AI_TOOL_CALL_ID: call.call_id,
                    GEN_AI_TOOL_DESCRIPTION: call.description or call.tool,
                },
            ))

        # Same rule as run_step: a repeatedly-failing tool ends the run rather than
        # being routed around.
        if failures is not None:
            cap = self._tool_failure_limits.get(call.tool)
            if cap is not None and failures > cap:
                raise ToolFailureLimitExceeded(call.tool, failures, cap) from error

    def warn_if_tool_limits_unenforced(self) -> None:
        """Called when the operation ends: a per-tool cap that was set but never had
        a governed tool to enforce it against did nothing, and the caller should hear
        about that rather than assume it applied."""
        if self._harness_observer_active:
            # A harness is running, so its own limiter enforces per-tool caps — see
            # harness_call_limits(). Failure budgets still ride on the middleware's
            # post-execution record, which only fires once it's installed.
            return
        unenforced = sorted(
            (set(self._tool_limits) | set(self._tool_failure_limits)) - self._governed_tools)
        if unenforced:
            log.warning(
                "agent %r set tool_call_limits for %s, but those tools weren't passed "
                "through ctx.agent_tools() — BoundFlow never dispatched them, so the "
                "limits were NOT enforced. Wrap them with ctx.agent_tools() (or use "
                "ctx.run_agent()) to enforce per-tool caps.",
                self.agent_name, ", ".join(unenforced))

    def snapshot(self) -> dict:
        """This agent's metrics for the operation, in the shape the server appends
        to invocation_metrics (mirrors what run_agent writes)."""
        return {
            "cost_usd": self.cost_usd,
            # The harness's count when it's metering, since it also sees calls that
            # never reached the governor.
            "llm_calls": (self.observed_llm_calls if self._harness_metering_active
                          else self.llm_calls),
            "tokens_used": self.tokens_used,
            "calls_per_tool": dict(self.calls_per_tool),
            # Only populated for tools passed through agent_tools() — we can't see a
            # failure in a tool we didn't dispatch.
            "tool_failure_counts": dict(self.tool_failure_counts),
            "latency_seconds": (now_ms() - self._start_ms) / 1000.0,
            "ran_at": int(time.time() * 1000),
        }

    def trace(self, output: Any = None):
        """This agent's run as an AgentRunTrace, for the operation trace."""
        from .trace import AgentRunTrace
        return AgentRunTrace(
            agent=self.agent_name,
            model=self.model,
            start_ms=self._start_ms,
            end_ms=now_ms(),
            spans=self.spans,
            output=output,
            cost_usd=self.cost_usd,
            tokens=self.tokens_used,
            llm_calls=self.llm_calls,
        )


def gen_ai_message(role: str, *, text: str = "", tool_calls: list | None = None) -> dict:
    """Build a canonical GenAI message from plain parts, so adapters don't need to
    construct BoundFlow's internal block types just to emit a span."""
    from .trace import PART_TEXT, PART_TOOL_CALL
    parts: list[dict] = []
    if text:
        parts.append({"type": PART_TEXT, "content": text})
    for tc in tool_calls or []:
        parts.append({
            "type": PART_TOOL_CALL,
            "id": tc.get("id", ""),
            "name": tc.get("name", ""),
            "arguments": tc.get("args") or {},
        })
    return {"role": role, "parts": parts}


def assistant_message(text: str = "", tool_calls: list | None = None) -> dict:
    return gen_ai_message(ROLE_ASSISTANT, text=text, tool_calls=tool_calls)
