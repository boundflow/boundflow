"""Getters for the *armed* policy config — the inverse of the setters. These read back
what's currently configured (not firings, and not a per-run snapshot). Pure control
plane: no worker, no LLM."""
from __future__ import annotations

from boundflow import (
    Cooldown,
    Pause,
    RuntimePolicy,
    SetVersion,
    WorkflowConfig,
    WorkflowMetric,
    WorkflowRule,
)

from .conftest import create_isolated_tenant


async def test_workflow_lifecycle_policy_round_trips(cp):
    tenant = await create_isolated_tenant(cp, "get-wf-policy")
    wf = await cp.create_workflow("wf_policy", tenant.id, config=WorkflowConfig(version=1))

    # Empty before anything is set.
    assert await cp.get_workflow_lifecycle_policy(wf.id) == []

    rules = [
        WorkflowRule(metric=WorkflowMetric.COST, threshold=5.0, action=SetVersion(target=1)),
        WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=3, action=Cooldown(window=2, seconds=10)),
        WorkflowRule(metric=WorkflowMetric.NUM_LLM_CALLS, threshold=100, action=Pause(window=4)),
    ]
    await cp.set_workflow_lifecycle_policy(wf.id, rules)

    got = await cp.get_workflow_lifecycle_policy(wf.id)
    assert got == rules  # full round-trip: metric, threshold, action type + params, window


async def test_agent_runtime_policy_round_trips(cp):
    tenant = await create_isolated_tenant(cp, "get-agent-policy")
    wf = await cp.create_workflow("agent_policy", tenant.id, config=WorkflowConfig(version=1))

    # Unset agent → empty dict, not an error.
    assert await cp.get_agent_runtime_policy(wf.id, "analyst") == {}

    await cp.set_agent_runtime_policy(
        wf.id, "analyst",
        RuntimePolicy(max_cost_usd=0.05, max_llm_calls=8, max_tokens_per_call=1024, model="claude-haiku-4-5"))

    got = await cp.get_agent_runtime_policy(wf.id, "analyst")
    assert got["max_cost_usd"] == 0.05
    assert got["max_llm_calls"] == 8
    assert got["max_tokens_per_call"] == 1024
    assert got["model"] == "claude-haiku-4-5"


async def test_agent_lifecycle_policy_empty_when_unset(cp):
    tenant = await create_isolated_tenant(cp, "get-agent-lc")
    wf = await cp.create_workflow("agent_lc", tenant.id, config=WorkflowConfig(version=1))
    assert await cp.get_agent_lifecycle_policy(wf.id, "analyst") == {}
