"""Unit tests for Orchestrator.run_step's max_llm_calls enforcement.

Regression coverage for the bug where the "grace" call granted to let the model
finalize via submit_result could compound past a single extra call — end_turn
nudges kept bumping the effective cap, so a non-compliant model could loop
forever, using unbounded calls. Fixed by forcing submit_result on the *last*
allowed call (a true hard cap on total calls) and raising
AgentPolicyLimitExceeded if that forced call still doesn't comply, instead of
granting another one. These run directly against Orchestrator + MockLlmClient,
no worker/control-plane machinery needed.
"""
from __future__ import annotations

import pytest

from boundflow import AgentPolicyLimitExceeded, MockLlmClient, RuntimePolicy, Tool, Turn, turn
from boundflow.llm import AgentStepConfig, Orchestrator


def _never_submitting_agent_config(policy: RuntimePolicy) -> AgentStepConfig:
    async def ping_handler(_):
        return "pong"

    return AgentStepConfig(
        objective="loop forever",
        system_prompt="mock agent",
        policy=policy,
        model="mock-model",
        tools=[Tool("ping", "ping", ping_handler)],
        output_schema={"done": {"type": "boolean"}},
    )


async def test_max_llm_calls_is_a_true_hard_cap():
    """The mock never calls submit_result on its own; the orchestrator's forced
    submit_result (via forced_tool on the last allowed call) must end the run at
    exactly max_llm_calls total calls, not max_llm_calls + 1."""
    def mock_fn(ctx) -> Turn:
        return turn(100, 50, "ping")

    orch = Orchestrator(MockLlmClient(mock_fn))
    cfg = _never_submitting_agent_config(RuntimePolicy(max_llm_calls=3))

    result = await orch.run_step(cfg)

    assert result.llm_calls_used == 3, f"expected exactly max_llm_calls(3) total calls, got {result.llm_calls_used}"


async def test_max_llm_calls_of_one_forces_submit_on_the_only_call():
    def mock_fn(ctx) -> Turn:
        return turn(100, 50, "ping")

    orch = Orchestrator(MockLlmClient(mock_fn))
    cfg = _never_submitting_agent_config(RuntimePolicy(max_llm_calls=1))

    result = await orch.run_step(cfg)

    assert result.llm_calls_used == 1


async def test_end_turn_nudge_does_not_compound_past_the_cap():
    """A model that hits end_turn (without calling submit_result) gets nudged,
    but that doesn't grant it extra calls beyond the cap — the top-of-loop check
    still forces submit_result once llm_calls reaches max_llm_calls - 1. Before
    the fix, each end_turn bumped max_llm_calls again, so this could compound
    into unbounded calls."""
    calls = {"n": 0}

    def mock_fn(ctx) -> Turn:
        calls["n"] += 1
        # Ends every turn without any tool call, mimicking a model that drifts
        # to end_turn instead of calling submit_result.
        return turn(100, 50)

    orch = Orchestrator(MockLlmClient(mock_fn))
    cfg = _never_submitting_agent_config(RuntimePolicy(max_llm_calls=3))

    result = await orch.run_step(cfg)

    # Calls 1-2 end in end_turn and get nudged (still within budget, not
    # forced). Call 3 is the last allowed call — forced_tool=submit_result — and
    # MockLlmClient honors forced_tool directly without invoking mock_fn again.
    assert calls["n"] == 2, f"mock_fn should be consulted for the 2 non-forced calls only, got {calls['n']}"
    assert result.llm_calls_used == 3, f"expected exactly 3 total calls, got {result.llm_calls_used}"


async def test_model_ignoring_forced_tool_choice_raises_instead_of_looping():
    """A fake client that always ends its turn, even when forced_tool is set —
    simulating a provider/model that doesn't honor forced tool choice (or a
    max_tokens truncation on the forced call, which anthropic_client.py maps to
    end_turn). Must raise, not grant another grace call."""
    calls = {"n": 0}

    class IgnoresForcedToolClient:
        async def complete(self, request):
            from boundflow.llm import LlmResponse, Usage
            calls["n"] += 1
            return LlmResponse(content=[], stop_reason="end_turn", usage=Usage(10, 10))

    orch = Orchestrator(IgnoresForcedToolClient())
    cfg = _never_submitting_agent_config(RuntimePolicy(max_llm_calls=2))

    with pytest.raises(AgentPolicyLimitExceeded):
        await orch.run_step(cfg)

    # Call 1 ends in end_turn and gets the nudge (not forced yet, since
    # llm_calls=0 < max_llm_calls-1=1). Call 2 is the forced-finalize call
    # (llm_calls=1 >= 1); it also ends in end_turn, so the orchestrator raises
    # instead of granting a third call.
    assert calls["n"] == 2, f"expected exactly 2 calls before raising, got {calls['n']}"


async def test_max_cost_usd_forcing_also_raises_instead_of_looping_on_noncompliance():
    """The max_cost_usd path reuses the same forced-call machinery as
    max_llm_calls, so a model that ignores the forced tool_choice after a cost
    cap is hit must also raise, not loop."""
    class IgnoresForcedToolClient:
        async def complete(self, request):
            from boundflow.llm import LlmResponse, ToolUseBlock, Usage
            if request.forced_tool:
                return LlmResponse(content=[], stop_reason="end_turn", usage=Usage(10, 10))
            return LlmResponse(
                content=[ToolUseBlock("id1", "ping", {})], stop_reason="tool_use",
                usage=Usage(1_000_000, 1_000_000))

    orch = Orchestrator(IgnoresForcedToolClient())
    policy = RuntimePolicy(max_cost_usd=0.01, model="mock-model")
    cfg = _never_submitting_agent_config(policy)
    cfg.pricing = {"mock-model": {"input_per_1m": 3.0, "output_per_1m": 15.0}}

    with pytest.raises(AgentPolicyLimitExceeded):
        await orch.run_step(cfg)
