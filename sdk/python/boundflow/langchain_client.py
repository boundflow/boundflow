"""LangChain adapter — run BoundFlow agents through any LangChain chat model.

`LangChainLlmClient` implements the `LlmClient` protocol by delegating to a
LangChain chat model (a `langchain_core.language_models.BaseChatModel`), so a
BoundFlow agent (`ctx.run_agent`) runs under BoundFlow's governance — per-run
cost caps, LLM-call limits, model policies, metrics, and tracing — while using
LangChain's provider ecosystem for the actual model calls.

    from langchain_anthropic import ChatAnthropic
    from boundflow.langchain_client import LangChainLlmClient

    worker = BoundFlowWorker(llm=LangChainLlmClient(ChatAnthropic(model="claude-haiku-4-5")))

Pass a *factory* — ``lambda name: ChatAnthropic(model=name)`` — instead of a model
instance to let agent model-switching policies (`SetModel`) choose the model per
run: the factory receives the policy-resolved model name.

Requirements and caveats:
- The model must support **tool calling** (the agent loop drives tools plus a
  `submit_result` tool for structured output).
- Token usage from `usage_metadata` drives cost accounting, so set
  `AgentDefinition.model` to the model you're actually using. A model that reports
  *no* usage fails loud as a `PlatformError` — BoundFlow won't run uncosted and
  escape its cost caps. Major providers (Anthropic, OpenAI, Google, Bedrock)
  report usage; verify yours does before relying on cost-based policies.
- The `max_tokens_per_call` cap is passed via `.bind(max_tokens=...)`, honored by
  providers that take a `max_tokens` param (most do).
- Prompt caching (`request.cache`) is not plumbed through — there's no
  provider-agnostic caching API in LangChain — so it's left to the model.

Requires `langchain-core` (`pip install "boundflow[langchain]"`); it's imported
lazily so this module can be imported without it.
"""
from __future__ import annotations

from typing import Any

from .errors import PlatformError
from .llm import (
    LlmRequest,
    LlmResponse,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    Usage,
)


def _to_lc_messages(request: LlmRequest) -> list:
    """LlmRequest.messages (BoundFlow block protocol) -> LangChain messages."""
    from langchain_core.messages import (
        AIMessage,
        HumanMessage,
        SystemMessage,
        ToolMessage,
    )

    out: list = []
    if request.system:
        out.append(SystemMessage(content=request.system))
    for m in request.messages:
        content = m.content
        if not isinstance(content, list):  # plain string content
            out.append(AIMessage(content=content) if m.role == "assistant"
                       else HumanMessage(content=content))
            continue
        if m.role == "assistant":
            text = "\n".join(b.text for b in content if isinstance(b, TextBlock))
            tool_calls = [{"name": b.name, "args": b.input, "id": b.id}
                          for b in content if isinstance(b, ToolUseBlock)]
            out.append(AIMessage(content=text, tool_calls=tool_calls))
        else:  # user turn: text becomes Human, tool results become ToolMessages
            for b in content:
                if isinstance(b, TextBlock):
                    out.append(HumanMessage(content=b.text))
                elif isinstance(b, ToolResultBlock):
                    out.append(ToolMessage(
                        content=b.content, tool_call_id=b.tool_use_id,
                        status="error" if b.is_error else "success"))
    return out


def _to_openai_tools(request: LlmRequest) -> list:
    """ToolSpec -> OpenAI-format function tools, which `bind_tools` normalizes for
    every LangChain provider."""
    return [
        {"type": "function",
         "function": {"name": t.name, "description": t.description, "parameters": t.input_schema}}
        for t in request.tools
    ]


def _extract_text(content: Any) -> str:
    """AIMessage.content may be a string or a list of content parts."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for c in content:
            if isinstance(c, str):
                parts.append(c)
            elif isinstance(c, dict) and c.get("type") == "text":
                parts.append(c.get("text", ""))
        return "".join(parts)
    return ""


class LangChainLlmClient:
    """Implements `LlmClient` by delegating to a LangChain chat model (or a
    factory of one). See the module docstring for usage."""

    def __init__(self, model: Any) -> None:
        # A LangChain chat model, or a callable(model_name) -> chat model.
        self._model = model

    def _resolve(self, model_name: str):
        m = self._model
        # A factory is callable but has no `ainvoke`; a chat model has `ainvoke`.
        if callable(m) and not hasattr(m, "ainvoke"):
            return m(model_name)
        return m

    async def complete(self, request: LlmRequest) -> LlmResponse:
        model = self._resolve(request.model)
        if request.tools:
            if request.forced_tool:
                model = model.bind_tools(_to_openai_tools(request),
                                         tool_choice=request.forced_tool)
            else:
                model = model.bind_tools(_to_openai_tools(request))
        # Per-call token cap (max_tokens_per_call policy); .bind() merges into the
        # RunnableBinding from bind_tools, so it composes with the tools.
        if request.max_tokens:
            model = model.bind(max_tokens=request.max_tokens)

        msg = await model.ainvoke(_to_lc_messages(request))

        content: list = []
        text = _extract_text(msg.content)
        if text:
            content.append(TextBlock(text))
        tool_calls = getattr(msg, "tool_calls", None) or []
        for i, tc in enumerate(tool_calls):
            content.append(ToolUseBlock(
                id=tc.get("id") or f"call_{i}",
                name=tc["name"],
                input=tc.get("args") or {},
            ))

        um = getattr(msg, "usage_metadata", None) or {}
        input_tokens = int(um.get("input_tokens", 0) or 0)
        output_tokens = int(um.get("output_tokens", 0) or 0)
        # No usage means BoundFlow can't price the run or enforce cost caps, so fail loud
        # rather than run ungoverned (PlatformError interrupts the workflow).
        if input_tokens == 0 and output_tokens == 0:
            raise PlatformError(
                f"LangChain model {type(model).__name__!r} returned no token usage "
                "(usage_metadata); BoundFlow cannot enforce cost governance for this run. "
                "Use a provider/model that reports usage, or a native BoundFlow client."
            )
        usage = Usage(input_tokens=input_tokens, output_tokens=output_tokens)
        # The loop only understands tool_use / end_turn (mirrors AnthropicLlmClient).
        stop_reason = "tool_use" if tool_calls else "end_turn"
        return LlmResponse(content=content, stop_reason=stop_reason, usage=usage)
