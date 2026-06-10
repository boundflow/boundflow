"""LLM message protocol, the scripted mock, and the orchestrator loop.

Provider-agnostic: the orchestrator speaks the small message protocol below, so
the mock and a real Anthropic client are interchangeable. The loop is a direct
port of BoundFlow.SDK Orchestrator.RunAsync.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from .policies import RuntimePolicy

INPUT_COST_PER_1M = 3.0
OUTPUT_COST_PER_1M = 15.0
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


@dataclass
class LlmResponse:
    content: list  # list of *Block
    stop_reason: str
    usage: Usage


class LlmClient(Protocol):
    async def complete(self, request: LlmRequest) -> LlmResponse: ...


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


@dataclass
class StepResult:
    output: dict | None
    llm_calls_used: int
    cost_usd: float
    tokens_used: int
    calls_per_tool: dict[str, int]
    tool_failure_counts: dict[str, int]
    model_used: str


def _estimate_cost(usage: Usage) -> float:
    return usage.input_tokens / 1_000_000 * INPUT_COST_PER_1M + \
        usage.output_tokens / 1_000_000 * OUTPUT_COST_PER_1M


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
        call_counts: dict[str, int] = {}
        failure_counts: dict[str, int] = {}
        tool_limits = {l.tool: l.max_calls for l in cfg.policy.tool_call_limits}

        while True:
            limit_reached = max_llm_calls > 0 and llm_calls >= max_llm_calls
            req = LlmRequest(
                model=cfg.model,
                max_tokens=4096,
                system=cfg.system_prompt + "\n\nWhen you have completed your objective, call submit_result.",
                messages=messages,
                tools=tools,
                forced_tool=SUBMIT_RESULT if limit_reached else None,
            )
            resp = await self._client.complete(req)

            llm_calls += 1
            cost += _estimate_cost(resp.usage)
            tokens += resp.usage.input_tokens + resp.usage.output_tokens
            messages.append(Message("assistant", resp.content))

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
                    return StepResult(block.input, llm_calls, cost, tokens,
                                      call_counts, failure_counts, cfg.model)

                if block.name not in callbacks:
                    tool_results.append(ToolResultBlock(block.id, f"Unknown callback: {block.name}", is_error=True))
                    continue

                cap = tool_limits.get(block.name, 0)
                if cap > 0 and call_counts.get(block.name, 0) >= cap:
                    tool_results.append(ToolResultBlock(
                        block.id, f"Call limit reached for '{block.name}' (max {cap}). Do not call it again.",
                        is_error=True))
                    continue
                call_counts[block.name] = call_counts.get(block.name, 0) + 1

                try:
                    out = await callbacks[block.name].handler(block.input)
                except Exception as ex:  # noqa: BLE001 — report tool failure to the model
                    failure_counts[block.name] = failure_counts.get(block.name, 0) + 1
                    tool_results.append(ToolResultBlock(block.id, str(ex), is_error=True))
                    continue

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
