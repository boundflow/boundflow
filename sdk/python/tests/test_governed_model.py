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
    tool_name: str = "ping"
    tool_args: dict = {}

    @property
    def _llm_type(self) -> str:
        return "fake"

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        raise NotImplementedError

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        self.calls.append(kwargs)
        tool_calls = ([{"name": self.tool_name, "args": dict(self.tool_args),
                        "id": f"tc{len(self.calls)}"}]
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
    pytest.importorskip("langgraph")
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


# ── governed tools (ctx.agent_tools) ─────────────────────────────────────────


@pytest.fixture
def search_tool():
    """(tool, runs) — `runs` records every actual execution, so a test can tell
    'the model asked' apart from 'the tool ran'. Returned as a tuple because
    StructuredTool is a pydantic model and won't take an extra attribute."""
    from langchain_core.tools import tool

    runs: list[str] = []

    @tool
    def search(query: str) -> str:
        """Search for something."""
        runs.append(query)
        return f"results for {query}"

    return search, runs


def _wrap(gov, tools):
    from boundflow.langchain_client import governed_tools
    return governed_tools(gov, tools)


async def test_governed_tool_preserves_the_models_view_of_the_tool(search_tool):
    tool, _ = search_tool
    gov = _governor(RuntimePolicy())
    wrapped = _wrap(gov, [tool])[0]
    assert wrapped.name == tool.name
    assert wrapped.description == tool.description
    assert wrapped.args_schema is tool.args_schema


async def test_tool_call_limit_is_enforced_and_the_model_is_told(search_tool):
    tool, runs = search_tool
    gov = _governor(RuntimePolicy(tool_call_limits=[ToolCallLimit(tool="search", max_calls=2)]))
    wrapped = _wrap(gov, [tool])[0]

    outs = [await wrapped.ainvoke({"query": f"q{i}"}) for i in range(4)]

    assert len(runs) == 2, "the underlying tool must stop running at the cap"
    assert "Call limit reached for 'search' (max 2)" in outs[2]
    assert outs[2] == outs[3]
    # Same refusal run_agent gives, so the model reacts the same way.
    from boundflow.llm import tool_limit_message
    assert outs[2] == tool_limit_message("search", 2)


async def test_tool_failures_are_counted_and_re_raised(search_tool):
    from langchain_core.tools import tool

    @tool
    def broken(query: str) -> str:
        """Always fails."""
        raise RuntimeError("boom")

    gov = _governor(RuntimePolicy())
    wrapped = _wrap(gov, [broken])[0]
    with pytest.raises(Exception):
        await wrapped.ainvoke({"query": "x"})
    assert gov.tool_failure_counts == {"broken": 1}
    assert gov.snapshot()["tool_failure_counts"] == {"broken": 1}


async def test_governed_tools_emit_tool_spans(search_tool):
    tool, _ = search_tool
    gov = _governor(RuntimePolicy())
    wrapped = _wrap(gov, [tool])[0]
    await wrapped.ainvoke({"query": "x"})

    tool_spans = [s for s in gov.spans if s.kind == "tool"]
    assert len(tool_spans) == 1
    assert tool_spans[0].name == "search"
    assert tool_spans[0].attributes["gen_ai.tool.name"] == "search"


async def test_tool_calls_are_not_double_counted(search_tool):
    """The model asking for a tool and the tool running must count once, not twice."""
    tool, _ = search_tool
    gov = _governor(RuntimePolicy())
    wrapped = _wrap(gov, [tool])[0]

    # The model asks for it (observed on the LLM response)...
    gov.begin_call().record(Usage(10, 10), tool_calls=["search"])
    # ...and then it actually runs.
    await wrapped.ainvoke({"query": "x"})

    assert gov.calls_per_tool == {"search": 1}, \
        f"governed tools count at execution only, got {gov.calls_per_tool}"


async def test_ungoverned_tool_limits_warn_at_flush(caplog):
    gov = _governor(RuntimePolicy(tool_call_limits=[ToolCallLimit(tool="search", max_calls=1)]))
    with caplog.at_level("WARNING"):
        gov.warn_if_tool_limits_unenforced()
    assert "were NOT enforced" in caplog.text
    assert "agent_tools" in caplog.text


async def test_no_warning_once_the_tool_is_governed(search_tool, caplog):
    tool, _ = search_tool
    gov = _governor(RuntimePolicy(tool_call_limits=[ToolCallLimit(tool="search", max_calls=1)]))
    _wrap(gov, [tool])
    with caplog.at_level("WARNING"):
        gov.warn_if_tool_limits_unenforced()
    assert "NOT enforced" not in caplog.text


async def test_langgraph_respects_a_per_tool_cap_and_keeps_going(search_tool):
    """The end-to-end shape: LangGraph drives, the tool cap trips mid-graph, and the
    model is told rather than the run being killed."""
    pytest.importorskip("langgraph")
    from langgraph.prebuilt import create_react_agent

    tool, runs = search_tool
    gov = _governor(RuntimePolicy(
        max_llm_calls=4,
        tool_call_limits=[ToolCallLimit(tool="search", max_calls=1)],
    ))
    model = GovernedChatModel(
        governor=gov, chat_model=FakeChat(calls=[], tool_name="search",
                                          tool_args={"query": "x"}))
    agent = create_react_agent(model, _wrap(gov, [tool]))

    # The model keeps asking for `search`, so the LLM cap is what finally stops it —
    # but the per-tool cap has already stopped the tool from actually running.
    with pytest.raises(AgentPolicyLimitExceeded, match="max_llm_calls"):
        await agent.ainvoke({"messages": [HumanMessage(content="go")]})

    assert gov.llm_calls == 4
    assert len(runs) == 1, f"per-tool cap of 1 should have held, tool ran {len(runs)}x"
    assert gov.calls_per_tool == {"search": 1}


async def test_governed_tools_enforce_tool_failure_limits_like_run_agent(search_tool):
    """Parity check: tool_failure_limits ends the run in the governed path too. It
    used to be enforced only when BoundFlow owned the loop, so a policy set on a
    governed agent silently did nothing."""
    from boundflow import ToolFailureLimit, ToolFailureLimitExceeded
    from langchain_core.tools import tool as lc_tool

    @lc_tool
    def broken(query: str) -> str:
        """Always fails."""
        raise RuntimeError("upstream 500")

    gov = _governor(RuntimePolicy(
        tool_failure_limits=[ToolFailureLimit(tool="broken", max_failures=1)]))
    wrapped = _wrap(gov, [broken])[0]

    await_err = None
    try:
        await wrapped.ainvoke({"query": "a"})   # first failure: tolerated
    except Exception as e:
        await_err = e
    assert not isinstance(await_err, ToolFailureLimitExceeded), "one failure is under the cap"

    with pytest.raises(ToolFailureLimitExceeded) as exc:
        await wrapped.ainvoke({"query": "b"})   # second: over the cap
    assert exc.value.tool == "broken"
    assert isinstance(exc.value.__cause__, RuntimeError)


async def test_governed_zero_tool_cap_blocks_rather_than_uncapping(search_tool):
    """Same landmine as run_step: a remaining budget of 0 must block, not uncap."""
    tool, runs = search_tool
    gov = _governor(RuntimePolicy(tool_call_limits=[ToolCallLimit(tool="search", max_calls=0)]))
    wrapped = _wrap(gov, [tool])[0]

    out = await wrapped.ainvoke({"query": "x"})
    assert len(runs) == 0, "max_calls=0 must block the tool"
    assert "Call limit reached" in out


async def test_governance_holds_through_deepagents(search_tool):
    """The product claim, against a real opinionated harness rather than a bare loop.

    deepagents layers sub-agents, a filesystem and context management on top of the
    model it's handed — so if caps, metering and spans survive it unchanged, the
    "pick a harness like you pick a model" story holds for the tier people actually
    select rather than build.
    """
    pytest.importorskip("deepagents")
    from deepagents import create_deep_agent

    tool, runs = search_tool
    gov = _governor(RuntimePolicy(
        max_llm_calls=3,
        max_tokens_per_call=444,
        tool_call_limits=[ToolCallLimit(tool="search", max_calls=1)],
    ))
    inner = FakeChat(calls=[], tool_name="search", tool_args={"query": "x"})
    model = GovernedChatModel(governor=gov, chat_model=inner)

    agent = create_deep_agent(model=model, tools=_wrap(gov, [tool]),
                              system_prompt="You research things.")

    # The fake never stops asking for the tool, so the cap is what ends it.
    with pytest.raises(AgentPolicyLimitExceeded, match="max_llm_calls"):
        await agent.ainvoke({"messages": [HumanMessage(content="research foo")]})

    assert gov.llm_calls == 3, f"llm cap should hold at 3, got {gov.llm_calls}"
    assert len(runs) == 1, f"per-tool cap should hold at 1, got {len(runs)}"
    assert gov.cost_usd > 0, "calls through the harness must still be priced"
    assert inner.calls[-1]["max_tokens"] == 444, "policy max_tokens must reach the provider"
    assert [s.kind for s in gov.spans].count("tool") == 1, "tool spans recorded through the harness"


async def test_structured_output_works_through_the_governed_model():
    """`output_schema` has no equivalent in the governed path — there's no
    AgentDefinition — so customers get typed results from the harness's own API
    instead. It has to stay metered, or they'd be trading governance for types."""
    from pydantic import BaseModel, Field

    class Verdict(BaseModel):
        answer: str = Field(description="the answer")
        confident: bool = Field(description="whether the agent is confident")

    class SchemaAwareChat(FakeChat):
        async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
            self.calls.append(kwargs)
            tools = kwargs.get("tools") or []
            name = "Verdict"
            if tools and isinstance(tools[0], dict):
                name = tools[0].get("function", {}).get("name") or name
            msg = AIMessage(
                content="",
                tool_calls=[{"name": name, "args": {"answer": "42", "confident": True}, "id": "t1"}],
                usage_metadata={"input_tokens": 50, "output_tokens": 10, "total_tokens": 60})
            return ChatResult(generations=[ChatGeneration(message=msg)])

    gov = _governor(RuntimePolicy(max_llm_calls=5))
    inner = SchemaAwareChat(calls=[])
    model = GovernedChatModel(governor=gov, chat_model=inner)

    out = await model.with_structured_output(Verdict).ainvoke([HumanMessage(content="q")])

    assert isinstance(out, Verdict) and out.answer == "42"
    assert inner.calls[-1].get("tools"), "the schema has to reach the provider"
    assert gov.llm_calls == 1, "a structured call is still a governed call"
    assert gov.cost_usd > 0 and len(gov.spans) == 1
