"""LLM message protocol, the scripted mock, and the orchestrator loop.

Provider-agnostic: the orchestrator speaks the small message protocol below, so
the mock and a real Anthropic client are interchangeable. The loop is a direct
port of BoundFlow.SDK Orchestrator.RunAsync.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from .policies import RuntimePolicy
from .trace import (
    BF_COST_USD,
    GEN_AI_OP_CHAT,
    GEN_AI_OP_EXECUTE_TOOL,
    GEN_AI_OPERATION_NAME,
    GEN_AI_REQUEST_MAX_TOKENS,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_RESPONSE_FINISH_REASONS,
    GEN_AI_SYSTEM,
    GEN_AI_TOOL_CALL_ID,
    GEN_AI_TOOL_DESCRIPTION,
    GEN_AI_TOOL_NAME,
    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS,
    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS,
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_OUTPUT_TOKENS,
    PART_TEXT,
    PART_TOOL_CALL,
    PART_TOOL_CALL_RESPONSE,
    ROLE_ASSISTANT,
    ROLE_SYSTEM,
    SPAN_KIND_LLM,
    SPAN_KIND_TOOL,
    Span,
    now_ms,
)


def _gen_ai_system(model: str) -> str:
    """Best-effort provider for the gen_ai.system attribute, from the model id."""
    m = model.lower()
    if m.startswith("claude") or "anthropic" in m:
        return "anthropic"
    if m.startswith(("gpt", "o1", "o3", "o4")) or "openai" in m:
        return "openai"
    if m.startswith("gemini"):
        return "gcp.gemini"
    return "unknown"

log = logging.getLogger("boundflow.orchestrator")

# Prompt-cache cost multipliers vs. the base input rate (Anthropic, 5-min TTL):
# cache writes bill at 1.25x, reads at 0.1x. Per-model $/1M rates are supplied by
# the server at runtime (context["modelPricing"]) — there is no hardcoded table.
CACHE_WRITE_MULT = 1.25
CACHE_READ_MULT = 0.1
SUBMIT_RESULT = "submit_result"


# ── Message protocol ──────────────────────────────────────────────────────────


@dataclass
class TextBlock:
    text: str


@dataclass
class ToolUseBlock:
    id: str
    name: str
    input: dict


@dataclass
class ToolResultBlock:
    tool_use_id: str
    content: str
    is_error: bool = False


@dataclass
class Message:
    role: str  # "user" | "assistant"
    content: list  # list of *Block


@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0

    def total_tokens(self) -> int:
        return (self.input_tokens + self.output_tokens
                + self.cache_creation_input_tokens + self.cache_read_input_tokens)


@dataclass
class ToolSpec:
    name: str
    description: str
    input_schema: dict


@dataclass
class LlmRequest:
    model: str
    max_tokens: int
    system: str
    messages: list[Message]
    tools: list[ToolSpec]
    forced_tool: str | None = None  # set → model MUST call this tool
    cache: bool = False  # set → cache the stable prefix (system + tools)


@dataclass
class LlmResponse:
    content: list  # list of *Block
    stop_reason: str
    usage: Usage


class LlmClient(Protocol):
    async def complete(self, request: LlmRequest) -> LlmResponse: ...


class AgentCallTimeout(Exception):
    """Raised when an LLM call exceeds RuntimePolicy.max_call_seconds. A customer-
    domain failure like any other callback exception — the operation completes
    (marked failed) and the workflow stays active."""


# ── Scripted mock ─────────────────────────────────────────────────────────────


@dataclass
class ToolCall:
    tool: str
    input: dict | None = None


@dataclass
class Turn:
    tool_calls: list[ToolCall]
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class MockContext:
    turn_index: int
    system_prompt: str


def turn(input_tokens: int, output_tokens: int, *tools: str) -> Turn:
    return Turn([ToolCall(t) for t in tools], input_tokens, output_tokens)


def submit() -> Turn:
    return Turn([ToolCall(SUBMIT_RESULT, {"summary": "done"})])


_counter = 0


def _next_id() -> str:
    global _counter
    _counter += 1
    return f"toolu_{_counter}"


class MockLlmClient:
    """Deterministic stand-in. The delegate maps a MockContext to a Turn."""

    def __init__(self, next_turn: Callable[[MockContext], Turn]) -> None:
        self._next = next_turn

    async def complete(self, request: LlmRequest) -> LlmResponse:
        # Honor a forced tool choice (policy limit hit → orchestrator forces submit).
        if request.forced_tool:
            return _build([ToolCall(request.forced_tool, {})], 0, 0)
        # turn_index = assistant turns already in history (history grows by one
        # assistant message per prior model call), matching the .NET mock.
        idx = sum(1 for m in request.messages if m.role == "assistant")
        t = self._next(MockContext(idx, request.system))
        return _build(t.tool_calls, t.input_tokens, t.output_tokens)


def _build(tool_calls: list[ToolCall], in_tok: int, out_tok: int) -> LlmResponse:
    blocks = [ToolUseBlock(_next_id(), tc.tool, tc.input or {}) for tc in tool_calls]
    return LlmResponse(content=blocks, stop_reason="tool_use", usage=Usage(in_tok, out_tok))


# ── Orchestrator (agent step loop) ───────────────────────────────────────────


@dataclass
class AgentStepConfig:
    objective: str
    system_prompt: str
    policy: RuntimePolicy
    model: str
    tools: list  # boundflow.worker.Tool
    output_schema: dict | None = None
    llm_context: list = field(default_factory=list)  # (key, metadata, payload)
    # {model_id: {"input_per_1m": x, "output_per_1m": y}} — supplied by the server.
    pricing: dict = field(default_factory=dict)
    cache: bool = False  # opt-in prompt caching for this agent


@dataclass
class StepResult:
    output: dict | None
    llm_calls_used: int
    cost_usd: float
    tokens_used: int
    calls_per_tool: dict[str, int]
    tool_failure_counts: dict[str, int]
    model_used: str
    spans: list[Span] = field(default_factory=list)  # ordered LLM + tool spans for the run trace


def _part_from_block(b: Any) -> dict:
    """A message block -> a GenAI 'part' (text / tool_call / tool_call_response).
    This is the OTel GenAI message shape, used as our canonical content form so
    every sink (OTel, file, DB) emits interoperable, standard-shaped content."""
    if isinstance(b, TextBlock):
        return {"type": PART_TEXT, "content": b.text}
    if isinstance(b, ToolUseBlock):
        return {"type": PART_TOOL_CALL, "id": b.id, "name": b.name, "arguments": b.input}
    if isinstance(b, ToolResultBlock):
        return {"type": PART_TOOL_CALL_RESPONSE, "id": b.tool_use_id,
                "result": b.content, "is_error": b.is_error}
    return {"type": PART_TEXT, "content": repr(b)}


def _gen_ai_message(role: str, blocks: list) -> dict:
    return {"role": role, "parts": [_part_from_block(b) for b in blocks]}


def _gen_ai_input_messages(req: LlmRequest) -> list:
    """The full input as canonical GenAI messages: the system message, then the
    conversation as-sent."""
    msgs = [{"role": ROLE_SYSTEM, "parts": [{"type": PART_TEXT, "content": req.system}]}]
    msgs += [_gen_ai_message(m.role, m.content) for m in req.messages]
    return msgs


def _rate_for(model: str, pricing: dict) -> dict | None:
    """Effective per-1M rates for a model. Exact match first, then prefix — so a
    dated ID (claude-haiku-4-5-20251001) resolves to its alias entry."""
    if model in pricing:
        return pricing[model]
    for key, rate in pricing.items():
        if model.startswith(key):
            return rate
    return None


def _estimate_cost(usage: Usage, model: str, pricing: dict) -> float:
    """USD cost of one LLM call from exact token usage and server-supplied rates.
    Token counts are exact (Anthropic usage); only the $/token rate is tabular."""
    rate = _rate_for(model, pricing)
    if rate is None:
        log.warning("no pricing for model %s; recording cost as 0", model)
        return 0.0
    in_rate = rate.get("input_per_1m", 0.0)
    out_rate = rate.get("output_per_1m", 0.0)
    # Cache writes/reads bill against the input rate at fixed multipliers; plain
    # input_tokens are the uncached remainder, so these don't double-count.
    input_units = (usage.input_tokens
                   + usage.cache_creation_input_tokens * CACHE_WRITE_MULT
                   + usage.cache_read_input_tokens * CACHE_READ_MULT)
    return input_units / 1_000_000 * in_rate + usage.output_tokens / 1_000_000 * out_rate


def _wrap_schema(props: dict | None) -> dict:
    return {"type": "object", "properties": props or {}}


class Orchestrator:
    def __init__(self, client: LlmClient) -> None:
        self._client = client

    async def run_step(self, cfg: AgentStepConfig) -> StepResult:
        """Run the agentic loop to completion. The model may call allowed tools
        freely and calls submit_result when done; if a policy limit is hit first,
        one final forced submit_result is made. Port of Orchestrator.RunAsync."""
        callbacks = {t.name: t for t in cfg.tools}
        tools = [ToolSpec(t.name, t.description or t.name, _wrap_schema(t.input_schema)) for t in cfg.tools]
        tools.append(ToolSpec(SUBMIT_RESULT, "Call this when done to submit your final result.",
                              _wrap_schema(cfg.output_schema)))

        messages = [Message("user", [TextBlock(_user_content(cfg))])]
        llm_calls = 0
        cost = 0.0
        tokens = 0
        max_llm_calls = cfg.policy.max_llm_calls
        max_tokens = cfg.policy.max_tokens_per_call or 4096  # 0 = unset → default
        call_counts: dict[str, int] = {}
        failure_counts: dict[str, int] = {}
        tool_limits = {l.tool: l.max_calls for l in cfg.policy.tool_call_limits}
        spans: list[Span] = []  # ordered LLM + tool spans, captured for the run trace

        log.debug("run_step start: objective=%s model=%s max_llm_calls=%s tool_limits=%s",
                  cfg.objective, cfg.model, max_llm_calls, tool_limits)

        while True:
            limit_reached = max_llm_calls > 0 and llm_calls >= max_llm_calls
            req = LlmRequest(
                model=cfg.model,
                max_tokens=max_tokens,
                cache=cfg.cache,
                system=cfg.system_prompt + "\n\nWhen you have completed your objective, call submit_result.",
                messages=messages,
                tools=tools,
                forced_tool=SUBMIT_RESULT if limit_reached else None,
            )
            log.debug("llm_call #%d forced_tool=%s", llm_calls + 1, req.forced_tool)
            _llm_start = now_ms()
            try:
                if cfg.policy.max_call_seconds > 0:
                    resp = await asyncio.wait_for(self._client.complete(req), timeout=cfg.policy.max_call_seconds)
                else:
                    resp = await self._client.complete(req)
            except asyncio.TimeoutError:
                raise AgentCallTimeout(
                    f"LLM call exceeded max_call_seconds={cfg.policy.max_call_seconds}") from None
            _llm_end = now_ms()
            _input_messages = _gen_ai_input_messages(req)  # snapshot as-sent, before appending the reply

            llm_calls += 1
            call_cost = _estimate_cost(resp.usage, cfg.model, cfg.pricing)
            cost += call_cost
            tokens += resp.usage.total_tokens()
            messages.append(Message("assistant", resp.content))

            spans.append(Span(
                kind=SPAN_KIND_LLM, name=f"{GEN_AI_OP_CHAT} {cfg.model}", start_ms=_llm_start, end_ms=_llm_end,
                input=_input_messages,
                output=[_gen_ai_message(ROLE_ASSISTANT, resp.content)],
                attributes={
                    GEN_AI_OPERATION_NAME: GEN_AI_OP_CHAT,
                    GEN_AI_SYSTEM: _gen_ai_system(cfg.model),
                    GEN_AI_REQUEST_MODEL: cfg.model,
                    GEN_AI_REQUEST_MAX_TOKENS: max_tokens,
                    GEN_AI_USAGE_INPUT_TOKENS: resp.usage.input_tokens,
                    GEN_AI_USAGE_OUTPUT_TOKENS: resp.usage.output_tokens,
                    GEN_AI_USAGE_CACHE_CREATION_INPUT_TOKENS: resp.usage.cache_creation_input_tokens,
                    GEN_AI_USAGE_CACHE_READ_INPUT_TOKENS: resp.usage.cache_read_input_tokens,
                    GEN_AI_RESPONSE_FINISH_REASONS: [resp.stop_reason],
                    BF_COST_USD: call_cost,
                },
            ))

            if resp.stop_reason == "end_turn":
                messages.append(Message("user", [TextBlock("Please call submit_result with your findings.")]))
                max_llm_calls = llm_calls + 1
                continue
            if resp.stop_reason != "tool_use":
                raise RuntimeError(f"Unexpected stop reason: {resp.stop_reason}")

            tool_results: list = []
            for block in resp.content:
                if not isinstance(block, ToolUseBlock):
                    continue

                if block.name == SUBMIT_RESULT:
                    log.debug("submit_result: llm_calls=%d call_counts=%s", llm_calls, call_counts)
                    return StepResult(block.input, llm_calls, cost, tokens,
                                      call_counts, failure_counts, cfg.model, spans)

                if block.name not in callbacks:
                    tool_results.append(ToolResultBlock(block.id, f"Unknown callback: {block.name}", is_error=True))
                    continue

                cap = tool_limits.get(block.name, 0)
                current_count = call_counts.get(block.name, 0)
                if cap > 0 and current_count >= cap:
                    log.debug("tool_limit hit: tool=%s count=%d cap=%d", block.name, current_count, cap)
                    tool_results.append(ToolResultBlock(
                        block.id, f"Call limit reached for '{block.name}' (max {cap}). Do not call it again.",
                        is_error=True))
                    continue
                call_counts[block.name] = current_count + 1
                log.debug("tool_call: tool=%s count_after=%d cap=%s", block.name, current_count + 1, cap or "unlimited")

                _tool_start = now_ms()
                _tool_attrs = {
                    GEN_AI_OPERATION_NAME: GEN_AI_OP_EXECUTE_TOOL,
                    GEN_AI_TOOL_NAME: block.name,
                    GEN_AI_TOOL_CALL_ID: block.id,
                    GEN_AI_TOOL_DESCRIPTION: callbacks[block.name].description or block.name,
                }
                try:
                    out = await callbacks[block.name].handler(block.input)
                except Exception as ex:  # noqa: BLE001 — report tool failure to the model
                    failure_counts[block.name] = failure_counts.get(block.name, 0) + 1
                    spans.append(Span(kind=SPAN_KIND_TOOL, name=block.name, start_ms=_tool_start, end_ms=now_ms(),
                                      input=block.input, error=str(ex), attributes=_tool_attrs))
                    tool_results.append(ToolResultBlock(block.id, str(ex), is_error=True))
                    continue

                spans.append(Span(kind=SPAN_KIND_TOOL, name=block.name, start_ms=_tool_start, end_ms=now_ms(),
                                  input=block.input, output=out, attributes=_tool_attrs))
                import json
                tool_results.append(ToolResultBlock(block.id, json.dumps(out) if out is not None else "{}"))

            if tool_results:
                messages.append(Message("user", tool_results))

            if cfg.policy.max_cost_usd > 0 and cost > cfg.policy.max_cost_usd:
                max_llm_calls = llm_calls  # force submit on the next turn


def _user_content(cfg: AgentStepConfig) -> str:
    lines = [f"Objective: {cfg.objective}"]
    if cfg.llm_context:
        lines.append("\nContext:")
        for _key, metadata, payload in cfg.llm_context:
            lines.append(f"- {metadata}: {payload}")
    return "\n".join(lines)
