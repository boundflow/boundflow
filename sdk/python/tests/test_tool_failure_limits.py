"""tool_failure_limits — a repeatedly-failing tool ends the run.

BoundFlow counted tool failures but never acted on them: the orchestrator reports
each error to the model and lets it retry forever. TOOL_FAILURE_RATE only reacts
across *runs*. This is the in-loop cap, and it raises rather than just blocking the
tool, because an agent that carries on without a broken dependency produces an
answer built on a capability that isn't there.

Runs against the orchestrator directly — no backend, no provider key.
"""
from __future__ import annotations

import pytest

from boundflow import (
    RuntimePolicy,
    ToolCallLimit,
    ToolFailureLimit,
    ToolFailureLimitExceeded,
    Tool,
    Turn,
    MockLlmClient,
    turn,
)
from boundflow.llm import AgentStepConfig, Orchestrator, ToolCall


def _cfg(policy: RuntimePolicy, handler) -> AgentStepConfig:
    return AgentStepConfig(
        objective="loop", system_prompt="mock", policy=policy, model="mock-model",
        tools=[Tool("flaky", "a tool that may fail", handler)],
        output_schema={"done": {"type": "boolean"}})


def _always_calls_flaky() -> MockLlmClient:
    return MockLlmClient(lambda _: turn(10, 5, "flaky"))


async def test_failing_tool_ends_the_run_once_over_its_cap():
    calls = {"n": 0}

    async def broken(_):
        calls["n"] += 1
        raise RuntimeError("upstream 500")

    orch = Orchestrator(_always_calls_flaky())
    cfg = _cfg(RuntimePolicy(max_llm_calls=20,
                             tool_failure_limits=[ToolFailureLimit(tool="flaky", max_failures=2)]),
               broken)

    with pytest.raises(ToolFailureLimitExceeded) as exc:
        await orch.run_step(cfg)

    assert exc.value.tool == "flaky"
    assert exc.value.cap == 2
    assert calls["n"] == 3, f"two failures tolerated, the third ends it; ran {calls['n']}"


async def test_the_underlying_error_is_chained_as_the_cause():
    """The operator needs to know *why* the tool broke, not just that it did."""
    async def broken(_):
        raise RuntimeError("upstream 500")

    orch = Orchestrator(_always_calls_flaky())
    cfg = _cfg(RuntimePolicy(max_llm_calls=20,
                             tool_failure_limits=[ToolFailureLimit(tool="flaky", max_failures=0)]),
               broken)

    with pytest.raises(ToolFailureLimitExceeded) as exc:
        await orch.run_step(cfg)
    assert isinstance(exc.value.__cause__, RuntimeError)
    assert "upstream 500" in str(exc.value.__cause__)


async def test_zero_failures_tolerated_raises_on_the_first():
    async def broken(_):
        raise RuntimeError("nope")

    orch = Orchestrator(_always_calls_flaky())
    cfg = _cfg(RuntimePolicy(max_llm_calls=20,
                             tool_failure_limits=[ToolFailureLimit(tool="flaky", max_failures=0)]),
               broken)
    with pytest.raises(ToolFailureLimitExceeded):
        await orch.run_step(cfg)


async def test_failures_under_the_cap_are_still_just_reported_to_the_model():
    """Below the cap the old behaviour stands: the error goes back as a tool result
    so the model can adapt, and the run continues."""
    state = {"n": 0}

    async def flaky_once(_):
        state["n"] += 1
        if state["n"] == 1:
            raise RuntimeError("transient")
        return "ok"

    # Submits on the third turn, so the run ends normally rather than by cap.
    def script(ctx):
        return turn(10, 5, "flaky") if ctx.turn_index < 2 else Turn([ToolCall("submit_result", {"done": True})])

    orch = Orchestrator(MockLlmClient(script))
    cfg = _cfg(RuntimePolicy(max_llm_calls=20,
                             tool_failure_limits=[ToolFailureLimit(tool="flaky", max_failures=3)]),
               flaky_once)

    result = await orch.run_step(cfg)
    assert result.tool_failure_counts == {"flaky": 1}
    assert result.output == {"done": True}


async def test_no_failure_limit_means_unlimited_retries():
    """Absent from the policy = no cap, as before."""
    calls = {"n": 0}

    async def broken(_):
        calls["n"] += 1
        raise RuntimeError("always")

    orch = Orchestrator(_always_calls_flaky())
    cfg = _cfg(RuntimePolicy(max_llm_calls=4), broken)  # no tool_failure_limits

    result = await orch.run_step(cfg)  # ends on the LLM-call cap, not the tool
    assert calls["n"] >= 3
    assert result.tool_failure_counts["flaky"] >= 3


# ── the reset-bug fix: an explicit 0 blocks instead of meaning "unlimited" ────


async def test_a_zero_call_limit_blocks_the_tool_rather_than_uncapping_it():
    """A caller passing a *remaining* budget of 0 must get "blocked", not
    "unlimited" — the old `cap > 0` check read them the same way."""
    calls = {"n": 0}

    async def counted(_):
        calls["n"] += 1
        return "ok"

    def script(ctx):
        return turn(10, 5, "flaky") if ctx.turn_index < 2 else Turn([ToolCall("submit_result", {"done": True})])

    orch = Orchestrator(MockLlmClient(script))
    cfg = _cfg(RuntimePolicy(max_llm_calls=20,
                             tool_call_limits=[ToolCallLimit(tool="flaky", max_calls=0)]),
               counted)

    await orch.run_step(cfg)
    assert calls["n"] == 0, "max_calls=0 must block the tool, not uncap it"
