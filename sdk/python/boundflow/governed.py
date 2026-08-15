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


@dataclass
class GovernedCall:
    """One permitted model call. Returned by `AgentGovernor.begin_call()` with the
    policy-resolved parameters the adapter must honour, and closed by `record()`."""

    model: str
    max_tokens: int
    timeout_seconds: float  # 0 = unset (no per-call timeout)
    _governor: "AgentGovernor"
    _start_ms: int = field(default_factory=now_ms)
    _recorded: bool = False

    def record(
        self,
        usage: Usage,
        *,
        tool_calls: list[str] | None = None,
        input_messages: list | None = None,
        output_message: dict | None = None,
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
            self, usage, tool_calls or [], input_messages, output_message)


@dataclass
class GovernedToolCall:
    """One tool invocation. `denied` means the per-tool cap is spent and the tool
    must NOT run — hand `denial_message` back to the model as the tool's result,
    the same refusal `run_agent` returns."""

    tool: str
    denied: bool
    denial_message: str
    _governor: "AgentGovernor"
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
    ) -> None:
        self.agent_name = agent_name
        self.policy = policy
        # A SetModel lifecycle action overrides the caller's default.
        self.model = policy.model or default_model
        self._pricing = pricing or {}
        self._collect_spans = collect_spans

        self.llm_calls = 0
        self.cost_usd = 0.0
        self.tokens_used = 0
        self.calls_per_tool: dict[str, int] = {}
        self.tool_failure_counts: dict[str, int] = {}
        self.spans: list[Span] = []
        self._start_ms = now_ms()

        self._tool_limits = {l.tool: l.max_calls for l in policy.tool_call_limits}
        # Tools handed to us via agent_tools(): we dispatch these, so their caps are
        # enforceable and their calls are counted at execution rather than inferred
        # from what the model asked for.
        self._governed_tools: set[str] = set()

    def register_governed_tools(self, names: list[str]) -> None:
        self._governed_tools.update(names)

    @property
    def max_tokens(self) -> int:
        return self.policy.max_tokens_per_call or DEFAULT_MAX_TOKENS

    def begin_call(self) -> GovernedCall:
        """Authorise one model call. Raises `AgentPolicyLimitExceeded` if a cap is
        already spent — the caller's framework sees the exception and the workflow
        handler fails the run (a customer-domain failure, so the workflow stays
        active and the receipt records why)."""
        if self.policy.max_llm_calls > 0 and self.llm_calls >= self.policy.max_llm_calls:
            raise AgentPolicyLimitExceeded(
                f"agent {self.agent_name!r} reached max_llm_calls="
                f"{self.policy.max_llm_calls} (calls used: {self.llm_calls})")
        if self.policy.max_cost_usd > 0 and self.cost_usd >= self.policy.max_cost_usd:
            raise AgentPolicyLimitExceeded(
                f"agent {self.agent_name!r} reached max_cost_usd="
                f"{self.policy.max_cost_usd} (spent: ${self.cost_usd:.4f})")
        return GovernedCall(
            model=self.model,
            max_tokens=self.max_tokens,
            timeout_seconds=self.policy.max_call_seconds,
            _governor=self,
        )

    def _record(
        self,
        call: GovernedCall,
        usage: Usage,
        tool_calls: list[str],
        input_messages: list | None,
        output_message: dict | None,
    ) -> float:
        end_ms = now_ms()
        cost = _estimate_cost(usage, self.model, self._pricing)

        self.llm_calls += 1
        self.cost_usd += cost
        self.tokens_used += usage.total_tokens()
        for name in tool_calls:
            # A governed tool counts itself when it actually runs — counting the
            # model's *request* here too would double it, and would also count calls
            # that were denied by a cap or never dispatched.
            if name not in self._governed_tools:
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
                },
            ))

        log.debug("governed call: agent=%s model=%s calls=%d cost=%.6f tokens=%d",
                  self.agent_name, self.model, self.llm_calls, self.cost_usd, self.tokens_used)
        return cost

    def begin_tool_call(self, tool: str) -> GovernedToolCall:
        """Authorise one tool invocation against `tool_call_limits`.

        Unlike a model cap this doesn't raise: a spent per-tool cap is a signal to
        the *model*, not a failure of the run, so the caller returns
        `denial_message` as the tool's result and the model carries on without that
        tool — matching what `run_agent` does today."""
        cap = self._tool_limits.get(tool, 0)
        used = self.calls_per_tool.get(tool, 0)
        if cap > 0 and used >= cap:
            log.debug("tool_limit hit: agent=%s tool=%s count=%d cap=%d",
                      self.agent_name, tool, used, cap)
            return GovernedToolCall(tool=tool, denied=True,
                                    denial_message=tool_limit_message(tool, cap),
                                    _governor=self)
        self.calls_per_tool[tool] = used + 1
        return GovernedToolCall(tool=tool, denied=False, denial_message="", _governor=self)

    def _record_tool(self, call: GovernedToolCall, input: Any,
                     output: Any, error: BaseException | None) -> None:
        if error is not None:
            self.tool_failure_counts[call.tool] = self.tool_failure_counts.get(call.tool, 0) + 1
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
                    GEN_AI_TOOL_DESCRIPTION: call.tool,
                },
            ))

    def warn_if_tool_limits_unenforced(self) -> None:
        """Called when the operation ends: a per-tool cap that was set but never had
        a governed tool to enforce it against did nothing, and the caller should hear
        about that rather than assume it applied."""
        unenforced = sorted(set(self._tool_limits) - self._governed_tools)
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
            "llm_calls": self.llm_calls,
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
