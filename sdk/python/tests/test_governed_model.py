"""ctx.agent_model() — you drive the agent loop, BoundFlow governs the calls.

The inverse of ctx.run_agent(). These tests run against fakes (no backend, no
provider key): the point is that governance survives being driven by someone
else's loop — including a real LangGraph agent — and that the metrics BoundFlow
reports are the ones it actually observed.
"""
from __future__ import annotations

import pytest

from boundflow import AgentGovernor, AgentPolicyLimitExceeded, RuntimePolicy, ToolCallLimit
from boundflow.llm import Usage

langchain_core = pytest.importorskip("langchain_core")

from langchain_core.language_models.chat_models import BaseChatModel  # noqa: E402
from langchain_core.messages import AIMessage, HumanMessage  # noqa: E402
from langchain_core.outputs import ChatGeneration, ChatResult  # noqa: E402

from boundflow.langchain_client import GovernedChatModel  # noqa: E402

PRICING = {"fake-model-1": {"input_per_1m": 1.0, "output_per_1m": 5.0},
           "fake-model-2": {"input_per_1m": 2.0, "output_per_1m": 10.0}}


class FakeChat(BaseChatModel):
    """Records what it was called with; always answers with one tool call."""
    model_name: str = "fake-model-1"
    calls: list = []
    with_tool_call: bool = True

    @property
    def _llm_type(self) -> str:
        return "fake"

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        raise NotImplementedError

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        self.calls.append(kwargs)
        tool_calls = ([{"name": "ping", "args": {}, "id": f"tc{len(self.calls)}"}]
                      if self.with_tool_call else [])
        msg = AIMessage(
            content="ok",
            tool_calls=tool_calls,
            usage_metadata={"input_tokens": 100, "output_tokens": 20, "total_tokens": 120},
        )
        return ChatResult(generations=[ChatGeneration(message=msg)])


def _governor(policy: RuntimePolicy, model: str = "fake-model-1") -> AgentGovernor:
    return AgentGovernor("responder", policy, model, PRICING)


# ── the governance core, independent of any framework ────────────────────────


def test_begin_call_returns_policy_resolved_parameters():
    gov = _governor(RuntimePolicy(max_tokens_per_call=777, max_call_seconds=12))
    call = gov.begin_call()
    assert call.model == "fake-model-1"
    assert call.max_tokens == 777
    assert call.timeout_seconds == 12


def test_set_model_policy_overrides_the_default_model():
    """A SetModel lifecycle action already resolved into the policy wins over the
    model the caller passed."""
    gov = _governor(RuntimePolicy(model="fake-model-2"), model="fake-model-1")
    assert gov.begin_call().model == "fake-model-2"


def test_max_llm_calls_is_enforced_across_calls():
    gov = _governor(RuntimePolicy(max_llm_calls=2))
    for _ in range(2):
        gov.begin_call().record(Usage(100, 20))
    with pytest.raises(AgentPolicyLimitExceeded, match="max_llm_calls"):
        gov.begin_call()
    assert gov.llm_calls == 2


def test_max_cost_usd_is_enforced_once_spend_lands():
    gov = _governor(RuntimePolicy(max_cost_usd=0.0005))
    gov.begin_call().record(Usage(1000, 1000))  # 1e-3 + 5e-3 = well over
    with pytest.raises(AgentPolicyLimitExceeded, match="max_cost_usd"):
        gov.begin_call()


def test_no_caps_means_unlimited():
    gov = _governor(RuntimePolicy())
    for _ in range(5):
        gov.begin_call().record(Usage(10, 10))
    assert gov.llm_calls == 5


def test_recording_twice_is_a_bug_and_raises():
    gov = _governor(RuntimePolicy())
    call = gov.begin_call()
    call.record(Usage(10, 10))
    with pytest.raises(RuntimeError, match="twice"):
        call.record(Usage(10, 10))


def test_tool_call_limits_warn_because_they_cannot_be_enforced(caplog):
    """BoundFlow doesn't dispatch tools in this mode, so a per-tool cap silently
    wouldn't apply — the caller has to be told rather than left assuming."""
    with caplog.at_level("WARNING"):
        _governor(RuntimePolicy(tool_call_limits=[ToolCallLimit(tool="ping", max_calls=1)]))
    assert "tool_call_limits" in caplog.text
    assert "cannot be enforced" in caplog.text


def test_snapshot_reports_observed_usage():
    gov = _governor(RuntimePolicy())
    gov.begin_call().record(Usage(1000, 100), tool_calls=["ping", "search"])
    gov.begin_call().record(Usage(1000, 100), tool_calls=["ping"])
    snap = gov.snapshot()
    assert snap["llm_calls"] == 2
    assert snap["tokens_used"] == 2200
    assert snap["cost_usd"] == pytest.approx(0.003)
    # Tool calls can't be blocked here, but they are observed — so calls_per_tool
    # still feeds lifecycle rules.
    assert snap["calls_per_tool"] == {"ping": 2, "search": 1}


# ── the LangChain adapter ────────────────────────────────────────────────────


async def test_governed_model_meters_calls_and_forwards_policy_max_tokens():
    gov = _governor(RuntimePolicy(max_tokens_per_call=555))
    inner = FakeChat(calls=[])
    model = GovernedChatModel(governor=gov, chat_model=inner)

    assert isinstance(model, BaseChatModel), "LangGraph requires a real BaseChatModel"
    await model.ainvoke([HumanMessage(content="hi")])

    assert gov.llm_calls == 1
    assert gov.tokens_used == 120
    assert gov.cost_usd == pytest.approx(0.0002)
    assert gov.calls_per_tool == {"ping": 1}
    assert inner.calls[-1]["max_tokens"] == 555


async def test_governed_model_emits_a_span_per_call():
    gov = _governor(RuntimePolicy())
    model = GovernedChatModel(governor=gov, chat_model=FakeChat(calls=[]))
    await model.ainvoke([HumanMessage(content="hi")])

    assert len(gov.spans) == 1
    span = gov.spans[0]
    assert span.kind == "llm"
    assert span.attributes["gen_ai.request.model"] == "fake-model-1"
    assert span.attributes["gen_ai.usage.input_tokens"] == 100
    assert span.input, "input messages should be captured for the receipt"
    assert span.output, "output message should be captured for the receipt"


async def test_bind_tools_keeps_governance_in_the_call_path():
    """LangGraph calls bind_tools on the model. If that returned a binding around
    the *inner* model, every call would silently escape governance."""
    gov = _governor(RuntimePolicy(max_llm_calls=5))
    model = GovernedChatModel(governor=gov, chat_model=FakeChat(calls=[]))

    bound = model.bind_tools([
        {"type": "function", "function": {"name": "ping", "description": "p", "parameters": {}}}
    ])
    await bound.ainvoke([HumanMessage(content="hi")])
    await bound.ainvoke([HumanMessage(content="again")])

    assert gov.llm_calls == 2, "calls through a tool-bound model must still be metered"


async def test_cap_raises_out_of_the_model_so_the_caller_sees_it():
    gov = _governor(RuntimePolicy(max_llm_calls=1))
    model = GovernedChatModel(governor=gov, chat_model=FakeChat(calls=[]))
    await model.ainvoke([HumanMessage(content="one")])
    with pytest.raises(AgentPolicyLimitExceeded):
        await model.ainvoke([HumanMessage(content="two")])


async def test_streaming_warns_rather_than_silently_not_streaming(caplog):
    import boundflow.langchain_client as lc
    lc._stream_warned = False  # the warning is once-per-process
    gov = _governor(RuntimePolicy())
    model = GovernedChatModel(governor=gov, chat_model=FakeChat(calls=[]))

    with caplog.at_level("WARNING"):
        chunks = [c async for c in model.astream([HumanMessage(content="hi")])]

    assert "streaming" in caplog.text.lower()
    # The whole response arrives in one chunk rather than token-by-token (LangChain
    # appends its own empty end-of-stream marker, hence "exactly one with content").
    with_content = [c for c in chunks if c.content]
    assert len(with_content) == 1, f"expected one non-streamed chunk, got {len(with_content)}"
    assert gov.llm_calls == 1, "the fallback call is still metered"


async def test_langgraph_runaway_loop_is_stopped_by_the_cap():
    """The end-to-end promise: someone else's agent loop, running away, stopped by
    BoundFlow's cap — with the spend it burned still recorded."""
    from langchain_core.tools import tool
    from langgraph.prebuilt import create_react_agent

    runs: list[str] = []

    @tool
    def ping(value: str) -> str:
        """A no-op ping tool."""
        runs.append(value)
        return "pong"

    gov = _governor(RuntimePolicy(max_llm_calls=3))
    # Always asks for the tool again, so the graph would loop forever.
    model = GovernedChatModel(governor=gov, chat_model=FakeChat(calls=[]))
    agent = create_react_agent(model, [ping])

    with pytest.raises(AgentPolicyLimitExceeded):
        await agent.ainvoke({"messages": [HumanMessage(content="go")]})

    assert gov.llm_calls == 3, f"expected the cap to stop it at 3, got {gov.llm_calls}"
    assert gov.cost_usd > 0, "spend burned before the cap must still be recorded"
    assert len(gov.spans) == 3
