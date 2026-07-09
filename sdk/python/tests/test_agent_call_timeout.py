"""RuntimePolicy.max_call_seconds cancels an in-flight LLM call outright — unlike
max_llm_calls/max_cost_usd/tool_call_limits, which force a graceful submit_result on
the next turn, a hung call never reaches a next turn. The cancellation surfaces as an
ordinary customer-domain failure (AgentCallTimeout), same as any callback exception:
the operation completes (marked failed) and the workflow stays active."""
from __future__ import annotations

import asyncio

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    RunOutcome,
    RuntimePolicy,
    WorkflowConfig,
)

from .conftest import WORKER_ADDRESS, create_isolated_tenant, dummy_mock, run_worker, wait_for_completion

AGENT_NAME = "slow"


class SlowLlmClient:
    """An LlmClient whose complete() never returns in time — proves max_call_seconds
    actually cancels the in-flight call rather than merely capping call count."""

    async def complete(self, request):
        await asyncio.sleep(5)
        raise AssertionError("should have been cancelled by max_call_seconds")


async def test_slow_call_is_cancelled_and_run_fails(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, SlowLlmClient())

    @worker.workflow("agent_call_timeout", version=1)
    async def _entry(ctx):
        await ctx.run_agent(AgentDefinition(
            name=AGENT_NAME, system_prompt="x", model="mock-model",
            output_schema={"done": {"type": "boolean"}},
        ))
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "agent-call-timeout")
        wf = await cp.create_workflow("agent_call_timeout", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_runtime_policy(wf.id, AGENT_NAME, RuntimePolicy(max_call_seconds=1))
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid)
            assert info.run_outcome == RunOutcome.UNCAUGHT_OPERATION_EXCEPTION
            assert "max_call_seconds" in info.failure_reason
        finally:
            await cp.delete_workflow(wf.id)


async def test_fast_call_within_budget_is_unaffected(cp):
    """A generous max_call_seconds shouldn't interfere with a normal, fast run."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("agent_call_timeout_ok", version=1)
    async def _entry(ctx):
        await ctx.run_agent(AgentDefinition(
            name=AGENT_NAME, system_prompt="x", model="mock-model",
            output_schema={"done": {"type": "boolean"}},
        ))
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "agent-call-timeout-ok")
        wf = await cp.create_workflow("agent_call_timeout_ok", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_runtime_policy(wf.id, AGENT_NAME, RuntimePolicy(max_call_seconds=30))
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid)
            assert info.run_outcome == RunOutcome.SUCCESSFUL
        finally:
            await cp.delete_workflow(wf.id)
