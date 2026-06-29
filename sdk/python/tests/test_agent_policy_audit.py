"""Agent-lifecycle policy audit: when an agent's lifecycle rules change its effective
runtime policy for a run, the SDK reports the action and the server records a typed
agent policy_action audit row (base -> effective + the rules that fired), fetchable
via GetAgentPolicyAudit(workflow_id, agent_name).
"""
from __future__ import annotations

import asyncio

from boundflow import (
    AgentDefinition,
    AgentMetric,
    AgentRule,
    BoundFlowWorker,
    Complete,
    Op,
    SetModel,
    WorkflowConfig,
)

from .conftest import (
    HAIKU,
    SONNET,
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
)

AGENT = "analyst"


async def _wait_for_agent_audit(cp, workflow_id, agent, timeout: int = 15):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        records = await cp.get_agent_policy_audit(workflow_id, agent)
        if records:
            return records
        assert asyncio.get_event_loop().time() < deadline, "agent policy audit never appeared"
        await asyncio.sleep(0.3)


async def test_agent_setmodel_writes_typed_audit(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())  # mock submits → 1 llm call/run

    @worker.workflow("agent_audit_wf", version=1)
    async def _entry(ctx):
        await ctx.run_agent(AgentDefinition(
            name=AGENT, system_prompt="x", model=SONNET,
            output_schema={"done": {"type": "boolean"}}))
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "agent-audit")
        wf = await cp.create_workflow("agent_audit_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_lifecycle_policy(wf.id, AGENT, [
                AgentRule(metric=AgentMetric.LLM_CALLS, op=Op.GTE, threshold=1, window=1,
                          action=SetModel(value=HAIKU)),
            ])
            await cp.activate_workflow(wf.id)

            # Run 1: no history -> rule doesn't fire -> no audit.
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, wf.id)
            # Run 2: run-1 metrics (llm_calls>=1) fire the rule -> SetModel -> audit.
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, wf.id)

            records = await _wait_for_agent_audit(cp, wf.id, AGENT)
            assert len(records) == 1, f"expected one agent policy action, got {len(records)}"
            r = records[0]
            assert r.agent == AGENT
            assert r.actor == "system"
            assert r.occurred_at is not None
            # The effective-policy diff: base had no model override; effective is haiku.
            assert r.base_policy["model"] == ""
            assert r.effective_policy["model"] == HAIKU
            # The why: the rule that fired, identified by content.
            assert len(r.fired_rules) == 1
            fr = r.fired_rules[0]
            assert fr["metric"] == "llm_calls"
            assert fr["op"] == "greater_than_or_equal"
            assert fr["threshold"] == 1
            assert fr["action"] == {"field": "model", "value": HAIKU}
            assert fr["trigger_value"] >= 1
        finally:
            await cp.delete_workflow(wf.id)


async def test_no_agent_rule_change_writes_no_audit(cp):
    """An agent with no lifecycle policy never changes its effective policy -> no rows."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("agent_noaudit_wf", version=1)
    async def _entry(ctx):
        await ctx.run_agent(AgentDefinition(
            name=AGENT, system_prompt="x", model=SONNET,
            output_schema={"done": {"type": "boolean"}}))
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "agent-noaudit")
        wf = await cp.create_workflow("agent_noaudit_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, wf.id)
            await asyncio.sleep(1)  # give any (unexpected) audit write a chance
            assert await cp.get_agent_policy_audit(wf.id, AGENT) == []
        finally:
            await cp.delete_workflow(wf.id)
