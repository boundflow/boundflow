"""LangChain adapter — both directions between BoundFlow and LangChain.

Inbound (`LangChainLlmClient`): BoundFlow drives the agent loop and calls a
LangChain model for each step. Use with `ctx.run_agent()`.

Outbound (`GovernedChatModel`, via `ctx.agent_model()`): *you* drive the loop —
LangGraph, a chain, anything taking a `BaseChatModel` — and BoundFlow governs
each call underneath. Use when you want your own agent architecture with
BoundFlow's caps, metering, and receipts. See `boundflow.governed` for the
framework-agnostic core this is built on.

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


def _usage_from_message(msg: Any, model: Any) -> Usage:
    """Token usage off a LangChain AIMessage. No usage means BoundFlow can't price
    the call or enforce cost caps, so fail loud rather than run ungoverned
    (PlatformError interrupts the workflow)."""
    um = getattr(msg, "usage_metadata", None) or {}
    input_tokens = int(um.get("input_tokens", 0) or 0)
    output_tokens = int(um.get("output_tokens", 0) or 0)
    if input_tokens == 0 and output_tokens == 0:
        raise PlatformError(
            f"LangChain model {type(model).__name__!r} returned no token usage "
            "(usage_metadata); BoundFlow cannot enforce cost governance for this run. "
            "Use a provider/model that reports usage, or a native BoundFlow client."
        )
    return Usage(input_tokens=input_tokens, output_tokens=output_tokens)


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

        usage = _usage_from_message(msg, model)
        # The loop only understands tool_use / end_turn (mirrors AnthropicLlmClient).
        stop_reason = "tool_use" if tool_calls else "end_turn"
        return LlmResponse(content=content, stop_reason=stop_reason, usage=usage)


# ── Outbound: a governed model the caller's own loop drives ──────────────────


def _lc_to_gen_ai_messages(messages: list) -> list:
    """LangChain messages -> canonical GenAI messages, for the trace span. Mirrors
    `llm._gen_ai_input_messages`, but reading LangChain's types instead of
    BoundFlow's block protocol."""
    from .trace import (
        PART_TEXT,
        PART_TOOL_CALL,
        PART_TOOL_CALL_RESPONSE,
        ROLE_ASSISTANT,
        ROLE_SYSTEM,
        ROLE_TOOL,
        ROLE_USER,
    )

    role_by_type = {
        "system": ROLE_SYSTEM,
        "human": ROLE_USER,
        "ai": ROLE_ASSISTANT,
        "tool": ROLE_TOOL,
    }

    out = []
    for m in messages:
        role = role_by_type.get(getattr(m, "type", ""), ROLE_USER)
        parts: list[dict] = []
        text = _extract_text(getattr(m, "content", ""))
        if text:
            parts.append({"type": PART_TEXT, "content": text})
        for tc in getattr(m, "tool_calls", None) or []:
            parts.append({
                "type": PART_TOOL_CALL,
                "id": tc.get("id", ""),
                "name": tc.get("name", ""),
                "arguments": tc.get("args") or {},
            })
        if role == ROLE_TOOL:
            parts.append({
                "type": PART_TOOL_CALL_RESPONSE,
                "id": getattr(m, "tool_call_id", ""),
                "result": text,
                "is_error": getattr(m, "status", "") == "error",
            })
        out.append({"role": role, "parts": parts})
    return out


def _tool_call_names(msg: Any) -> list[str]:
    return [tc.get("name", "") for tc in (getattr(msg, "tool_calls", None) or [])]


_governed_cls = None
_stream_warned = False


def GovernedChatModel(*, governor: Any, chat_model: Any):
    """A `BaseChatModel` that runs every call through an `AgentGovernor` — caps
    enforced, cost and tokens metered, spans recorded — while behaving like an
    ordinary LangChain model to whatever drives it.

    Prefer `ctx.agent_model(...)`, which resolves the agent's policy and wires the
    governor for you.

    The class is built on first use so `langchain_core` stays an optional import,
    matching the rest of this module.
    """
    global _governed_cls
    if _governed_cls is None:
        _governed_cls = _build_governed_cls()
    return _governed_cls(governor=governor, chat_model=chat_model)


def _build_governed_cls():
    import asyncio
    import logging

    from langchain_core.language_models.chat_models import BaseChatModel
    from langchain_core.messages import AIMessageChunk
    from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult

    from .llm import AgentCallTimeout

    log = logging.getLogger("boundflow.governed")

    class _GovernedChatModel(BaseChatModel):
        # `Any` keeps pydantic from trying to validate/copy the governor or the
        # customer's model (which may not be a pydantic type at all).
        governor: Any
        chat_model: Any

        @property
        def _llm_type(self) -> str:
            return "boundflow_governed"

        def _resolve(self, model_name: str):
            m = self.chat_model
            # A factory is callable but has no `ainvoke`; a chat model has `ainvoke`.
            if callable(m) and not hasattr(m, "ainvoke"):
                return m(model_name)
            return m

        def bind_tools(self, tools, **kwargs):
            """Bind tools while keeping governance in the call path.

            This has to be implemented, not inherited: `BaseChatModel.bind_tools`
            raises NotImplementedError, and LangGraph calls it for every tool-calling
            agent. Formatting is delegated to the wrapped model (so provider-specific
            tool schemas still work), but the formatted kwargs are then bound to
            *self* — so calls route through `_agenerate` and stay metered, instead of
            binding to the inner model and silently escaping governance.
            """
            inner = self._resolve(self.governor.model)
            try:
                formatted = inner.bind_tools(tools, **kwargs)
                bound_kwargs = getattr(formatted, "kwargs", None) or {"tools": tools, **kwargs}
            except NotImplementedError:
                # Minimal/custom models may not implement bind_tools; pass the tools
                # through unformatted rather than failing outright.
                bound_kwargs = {"tools": tools, **kwargs}
            return self.bind(**bound_kwargs)

        async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
            # begin_call enforces the caps and hands back the policy-resolved model —
            # so a call can't be made without first passing the check.
            call = self.governor.begin_call()

            model = self._resolve(call.model)
            if call.max_tokens:
                model = model.bind(max_tokens=call.max_tokens)

            invoke = model.ainvoke(messages, stop=stop, **kwargs)
            if call.timeout_seconds > 0:
                try:
                    msg = await asyncio.wait_for(invoke, timeout=call.timeout_seconds)
                except asyncio.TimeoutError:
                    raise AgentCallTimeout(
                        f"LLM call exceeded max_call_seconds={call.timeout_seconds}") from None
            else:
                msg = await invoke

            call.record(
                _usage_from_message(msg, model),
                tool_calls=_tool_call_names(msg),
                input_messages=_lc_to_gen_ai_messages(messages),
                output_message=_lc_to_gen_ai_messages([msg])[0],
            )
            return ChatResult(generations=[ChatGeneration(message=msg)])

        def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
            raise NotImplementedError(
                "governed models are async-only — use ainvoke()/astream() (BoundFlow "
                "workflow handlers are async).")

        async def _astream(self, messages, stop=None, run_manager=None, **kwargs):
            # Streaming isn't governed yet: token usage typically only lands on the
            # final chunk and providers disagree about where, so metering a stream
            # isn't reliable enough to run cost caps against. Fall back to a single
            # non-streamed chunk — the LangChain default — but say so, since silently
            # not streaming is a confusing way to find out.
            global _stream_warned
            if not _stream_warned:
                _stream_warned = True
                log.warning(
                    "streaming through a governed model is not supported yet; "
                    "returning the full response as a single chunk. Cost governance "
                    "requires token usage, which streams don't reliably report.")
            result = await self._agenerate(messages, stop=stop, run_manager=run_manager, **kwargs)
            msg = result.generations[0].message
            yield ChatGenerationChunk(
                message=AIMessageChunk(
                    content=msg.content,
                    tool_calls=getattr(msg, "tool_calls", None) or [],
                    usage_metadata=getattr(msg, "usage_metadata", None),
                )
            )

    # Keep the public name on the built class, not the factory function.
    _GovernedChatModel.__name__ = "GovernedChatModel"
    _GovernedChatModel.__qualname__ = "GovernedChatModel"
    return _GovernedChatModel


_governed_tool_cls = None


def governed_tools(governor: Any, tools: list) -> list:
    """Wrap LangChain tools so BoundFlow dispatches them: per-tool caps enforced,
    failures counted, tool spans recorded. Prefer `ctx.agent_tools(...)`.

    Each wrapper keeps the original's name, description and args schema, so the
    model sees exactly the same tool it would have.
    """
    global _governed_tool_cls
    if _governed_tool_cls is None:
        _governed_tool_cls = _build_governed_tool_cls()
    governor.register_governed_tools([t.name for t in tools])
    return [
        _governed_tool_cls(
            name=t.name,
            description=t.description,
            args_schema=t.args_schema,
            governor=governor,
            inner=t,
        )
        for t in tools
    ]


def _build_governed_tool_cls():
    from langchain_core.tools import BaseTool

    class _GovernedTool(BaseTool):
        governor: Any = None
        inner: Any = None

        def _run(self, *args, **kwargs):
            raise NotImplementedError(
                "governed tools are async-only — BoundFlow workflow handlers are async.")

        async def _arun(self, *args, **kwargs):
            call = self.governor.begin_tool_call(self.name)
            if call.denied:
                # Not an error for the run: the model is told the tool is spent and
                # continues without it, exactly as it would under run_agent.
                return call.denial_message
            tool_input = kwargs if kwargs else (args[0] if args else {})
            try:
                output = await self.inner.ainvoke(tool_input)
            except Exception as exc:  # noqa: BLE001 — reported to the model, and counted
                call.record(input=tool_input, error=exc)
                raise
            call.record(input=tool_input, output=output)
            return output

    _GovernedTool.__name__ = "GovernedTool"
    _GovernedTool.__qualname__ = "GovernedTool"
    return _GovernedTool
