"""Real Anthropic API client implementing the LlmClient protocol."""
from __future__ import annotations

import anthropic as _anthropic

from .llm import LlmClient, LlmRequest, LlmResponse, TextBlock, ToolResultBlock, ToolUseBlock, Usage


def _encode(blocks: list) -> list:
    out = []
    for b in blocks:
        if isinstance(b, TextBlock):
            out.append({"type": "text", "text": b.text})
        elif isinstance(b, ToolUseBlock):
            out.append({"type": "tool_use", "id": b.id, "name": b.name, "input": b.input})
        elif isinstance(b, ToolResultBlock):
            out.append({"type": "tool_result", "tool_use_id": b.tool_use_id,
                        "content": b.content, "is_error": b.is_error})
    return out


def _decode(blocks) -> list:
    out = []
    for b in blocks:
        if b.type == "text":
            out.append(TextBlock(b.text))
        elif b.type == "tool_use":
            out.append(ToolUseBlock(b.id, b.name, b.input))
    return out


class AnthropicLlmClient:
    """Wraps anthropic.AsyncAnthropic to implement LlmClient."""

    def __init__(self, api_key: str) -> None:
        self._client = _anthropic.AsyncAnthropic(api_key=api_key)

    async def complete(self, request: LlmRequest) -> LlmResponse:
        messages = [
            {"role": m.role, "content": _encode(m.content) if isinstance(m.content, list) else m.content}
            for m in request.messages
        ]
        tools = [
            {"name": t.name, "description": t.description, "input_schema": t.input_schema}
            for t in request.tools
        ]
        kwargs: dict = dict(
            model=request.model,
            max_tokens=request.max_tokens,
            system=request.system,
            messages=messages,
            tools=tools,
        )
        if request.forced_tool:
            kwargs["tool_choice"] = {"type": "tool", "name": request.forced_tool}

        resp = await self._client.messages.create(**kwargs)
        stop = resp.stop_reason
        # Treat max_tokens like end_turn so the orchestrator can re-prompt with submit_result.
        if stop == "max_tokens":
            stop = "end_turn"
        return LlmResponse(
            content=_decode(resp.content),
            stop_reason=stop,
            usage=Usage(resp.usage.input_tokens, resp.usage.output_tokens),
        )
