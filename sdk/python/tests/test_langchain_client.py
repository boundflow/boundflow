"""Unit tests for the LangChain LlmClient adapter — the LlmRequest <-> LangChain
mapping and the response back to LlmResponse. No backend, no real model: a
duck-typed fake chat model returns scripted replies."""
from __future__ import annotations

import pytest

pytest.importorskip("langchain_core")

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    RunOutcome,
    WorkflowConfig,
)
from boundflow.langchain_client import LangChainLlmClient
from boundflow.llm import (
    LlmRequest,
    Message,
    TextBlock,
    ToolResultBlock,
    ToolSpec,
    ToolUseBlock,
)

from .conftest import (
    HAIKU,
    WORKER_ADDRESS,
    create_isolated_tenant,
    run_worker,
    wait_for_completion,
)


class _FakeChat:
    """Minimal chat model: records how it was bound/invoked and returns a scripted
    reply (a real langchain AIMessage). Has `ainvoke`, so the adapter treats it as a
    model rather than a factory."""

    def __init__(self, reply):
        self._reply = reply
        self.bound_tools = None
        self.tool_choice = "UNSET"
        self.bound_kwargs: dict = {}
        self.seen_messages = None

    def bind_tools(self, tools, tool_choice=None):
        self.bound_tools = tools
        self.tool_choice = tool_choice
        return self

    def bind(self, **kw):
        self.bound_kwargs.update(kw)
        return self

    async def ainvoke(self, messages, **kw):
        self.seen_messages = messages
        return self._reply


def _req(**kw) -> LlmRequest:
    base = dict(
        model="claude-haiku-4-5", max_tokens=1024, system="be helpful",
        messages=[Message("user", [TextBlock("hi")])],
        tools=[ToolSpec("submit_result", "finish", {"type": "object"})],
    )
    base.update(kw)
    return LlmRequest(**base)


async def test_tool_call_and_usage_map_to_llmresponse():
    from langchain_core.messages import AIMessage
    reply = AIMessage(
        content="",
        tool_calls=[{"name": "submit_result", "args": {"summary": "ok"}, "id": "c1"}],
        usage_metadata={"input_tokens": 100, "output_tokens": 20, "total_tokens": 120},
    )
    resp = await LangChainLlmClient(_FakeChat(reply)).complete(_req())

    assert resp.stop_reason == "tool_use"
    tools = [b for b in resp.content if isinstance(b, ToolUseBlock)]
    assert len(tools) == 1
    assert tools[0].name == "submit_result"
    assert tools[0].input == {"summary": "ok"}
    assert tools[0].id == "c1"
    assert resp.usage.input_tokens == 100
    assert resp.usage.output_tokens == 20


async def test_missing_usage_raises_platform_error():
    """A model that reports no token usage leaves BoundFlow unable to price the run or
    enforce cost caps — the adapter must fail loud (PlatformError) rather than let the
    run proceed ungoverned."""
    from langchain_core.messages import AIMessage
    from boundflow import PlatformError

    # A reply with no usage_metadata at all.
    reply = AIMessage(content="", tool_calls=[{"name": "submit_result", "args": {}, "id": "c1"}])
    with pytest.raises(PlatformError):
        await LangChainLlmClient(_FakeChat(reply)).complete(_req())

    # And an explicit zero-usage report is treated the same way.
    zero = AIMessage(
        content="hi",
        usage_metadata={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    )
    with pytest.raises(PlatformError):
        await LangChainLlmClient(_FakeChat(zero)).complete(_req())


def _usage(input_tokens=10, output_tokens=5):
    return {"input_tokens": input_tokens, "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens}


async def test_text_only_reply_is_end_turn():
    from langchain_core.messages import AIMessage
    reply = AIMessage(content="just text", usage_metadata=_usage())
    resp = await LangChainLlmClient(_FakeChat(reply)).complete(_req())
    assert resp.stop_reason == "end_turn"
    assert any(isinstance(b, TextBlock) and b.text == "just text" for b in resp.content)


async def test_forced_tool_passes_tool_choice_and_openai_tool_format():
    from langchain_core.messages import AIMessage
    fake = _FakeChat(AIMessage(content="", tool_calls=[{"name": "submit_result", "args": {}, "id": "c1"}],
                               usage_metadata=_usage()))
    await LangChainLlmClient(fake).complete(_req(forced_tool="submit_result"))
    assert fake.tool_choice == "submit_result"
    assert fake.bound_tools[0]["function"]["name"] == "submit_result"


async def test_tool_result_block_maps_to_toolmessage():
    from langchain_core.messages import AIMessage, ToolMessage
    fake = _FakeChat(AIMessage(content="done", usage_metadata=_usage()))
    req = _req(messages=[
        Message("user", [TextBlock("go")]),
        Message("assistant", [ToolUseBlock("t1", "search", {"q": "x"})]),
        Message("user", [ToolResultBlock("t1", "found")]),
    ])
    await LangChainLlmClient(fake).complete(req)
    tool_msgs = [m for m in fake.seen_messages if isinstance(m, ToolMessage)]
    assert len(tool_msgs) == 1
    assert tool_msgs[0].tool_call_id == "t1"


async def test_factory_receives_the_resolved_model_name():
    """A factory (callable without `ainvoke`) lets SetModel policies pick the model —
    it must be called with request.model."""
    from langchain_core.messages import AIMessage
    seen = {}

    def factory(name):
        seen["name"] = name
        return _FakeChat(AIMessage(content="ok", usage_metadata=_usage()))

    await LangChainLlmClient(factory).complete(_req(model="gpt-4o-mini"))
    assert seen["name"] == "gpt-4o-mini"


async def test_max_tokens_is_bound_per_call():
    from langchain_core.messages import AIMessage
    fake = _FakeChat(AIMessage(content="hi", usage_metadata=_usage()))
    await LangChainLlmClient(fake).complete(_req(max_tokens=512))
    assert fake.bound_kwargs.get("max_tokens") == 512


async def test_agent_completes_end_to_end_through_the_adapter(cp):
    """The adapter drives a real ctx.run_agent loop to completion against the live
    backend — a fake LangChain model returns submit_result, so no provider/key."""
    from langchain_core.messages import AIMessage

    class _FakeSubmitter:
        def bind_tools(self, tools, tool_choice=None):
            return self

        def bind(self, **kw):
            return self

        async def ainvoke(self, messages, **kw):
            return AIMessage(
                content="",
                tool_calls=[{"name": "submit_result", "args": {"summary": "done"}, "id": "c1"}],
                usage_metadata={"input_tokens": 50, "output_tokens": 10, "total_tokens": 60},
            )

    got: dict = {}
    worker = BoundFlowWorker(WORKER_ADDRESS, LangChainLlmClient(_FakeSubmitter()))

    @worker.workflow("lc_adapter", version=1)
    async def _entry(ctx):
        result = await ctx.run_agent(AgentDefinition(
            name="analyst", system_prompt="do it", model=HAIKU,
            output_schema={"summary": {"type": "string"}}))
        got["output"] = result.output
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "lc-adapter")
        wf = await cp.create_workflow("lc_adapter", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid)
            assert info.run_outcome == RunOutcome.SUCCESSFUL
            assert got["output"] == {"summary": "done"}
        finally:
            await cp.delete_workflow(wf.id)


async def test_missing_usage_interrupts_the_workflow_end_to_end(cp):
    """A model that reports no usage must fail the run as a platform interruption (not a
    customer failure): the request ends failed with run_outcome=interrupted and the
    PlatformError message surfaced as failure_reason — driven through the live backend."""
    from langchain_core.messages import AIMessage

    class _NoUsageModel:
        def bind_tools(self, tools, tool_choice=None):
            return self

        def bind(self, **kw):
            return self

        async def ainvoke(self, messages, **kw):
            # A plausible tool call, but no usage_metadata — cost can't be recorded.
            return AIMessage(
                content="",
                tool_calls=[{"name": "submit_result", "args": {"summary": "x"}, "id": "c1"}])

    worker = BoundFlowWorker(WORKER_ADDRESS, LangChainLlmClient(_NoUsageModel()))

    @worker.workflow("lc_nousage", version=1)
    async def _entry(ctx):
        await ctx.run_agent(AgentDefinition(
            name="analyst", system_prompt="do it", model=HAIKU,
            output_schema={"summary": {"type": "string"}}))
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "lc-nousage")
        wf = await cp.create_workflow("lc_nousage", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid)
            assert info.status == "failed"
            assert info.run_outcome == RunOutcome.INTERRUPTED
            assert "usage" in info.failure_reason.lower()
        finally:
            await cp.delete_workflow(wf.id)


async def test_real_chatanthropic_agent_runs(cp, api_key):
    """Live: a real langchain-anthropic ChatAnthropic drives an agent to completion
    through the adapter — exercising real bind_tools, tool_choice, tool-calling, and
    usage_metadata against an actual provider. Skipped without ANTHROPIC_API_KEY or
    langchain-anthropic installed."""
    lca = pytest.importorskip("langchain_anthropic")
    model = lca.ChatAnthropic(model=HAIKU, api_key=api_key, max_tokens=1024)

    worker = BoundFlowWorker(WORKER_ADDRESS, LangChainLlmClient(model))

    @worker.workflow("lc_live", version=1)
    async def _entry(ctx):
        ctx.add_context("text", "BoundFlow governs agent fleets: cost caps, approvals, and rollbacks.")
        result = await ctx.run_agent(AgentDefinition(
            name="summarizer", system_prompt="Summarize the provided text in one short sentence.",
            model=HAIKU, output_schema={"summary": {"type": "string"}}))
        assert isinstance(result.output.get("summary"), str) and result.output["summary"].strip()
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "lc-live")
        wf = await cp.create_workflow("lc_live", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid)
            assert info.run_outcome == RunOutcome.SUCCESSFUL
        finally:
            await cp.delete_workflow(wf.id)
