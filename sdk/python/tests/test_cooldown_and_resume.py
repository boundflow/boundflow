"""Port of CooldownAndResumeTests.cs"""
from __future__ import annotations

import asyncio
import time

import grpc
import pytest

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Cooldown,
    Complete,
    WorkflowConfig,
    WorkflowMetric,
    WorkflowRule,
    WorkflowState,
)
from boundflow.anthropic_client import AnthropicLlmClient

from .conftest import (
    HAIKU,
    WORKER_ADDRESS,
    create_isolated_tenant,
    run_worker,
    wait_for_completion,
    wait_for_workflow_state,
)

COOLDOWN_SECONDS = 8


def _agent():
    return AgentDefinition(
        name="analyst",
        system_prompt="You are a concise data analyst.",
        model=HAIKU,
        output_schema={"summary": {"type": "string"}},
    )


def _cooldown_policy():
    return [WorkflowRule(
        metric=WorkflowMetric.NUM_LLM_CALLS,
        threshold=1,
        action=Cooldown(window=1, seconds=COOLDOWN_SECONDS),
    )]


async def test_workflow_resumes_after_cooldown_expires(cp, api_key):
    """Workflow transitions back to Active automatically once the cooldown window expires."""
    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("cooldown_resume", version=1)
    async def _entry(ctx):
        ctx.add_context("task", "Summarize in one sentence: BoundFlow schedules agentic workflows.")
        await ctx.run_agent(_agent())
        return Complete()

    async with run_worker(worker):
        _, tenant = await create_isolated_tenant(cp, "cooldown-resume")
        workflow = await cp.create_workflow("cooldown_resume", tenant.id,
                                            config=WorkflowConfig(version=1))

        await cp.set_workflow_lifecycle_policy(workflow.id, _cooldown_policy())
        await cp.activate_workflow(workflow.id)

        await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
        await wait_for_completion(cp, workflow.id)
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.COOLDOWN)

        cooldown_entered_at = time.monotonic()
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.ACTIVE, timeout=60)
        elapsed = time.monotonic() - cooldown_entered_at

        state = await cp.get_workflow_state(workflow.id)
        assert state == WorkflowState.ACTIVE
        assert elapsed >= COOLDOWN_SECONDS, \
            f"Cooldown lasted {elapsed:.1f}s but was configured for {COOLDOWN_SECONDS}s"


async def test_invoke_while_in_cooldown_is_rejected_then_succeeds_after_resume(cp, api_key):
    """
    Invoking during Cooldown raises FAILED_PRECONDITION.
    After cooldown expires, a fresh invocation completes normally.
    """
    completed = []

    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("cooldown_invoke", version=1)
    async def _entry(ctx):
        ctx.add_context("task", "Summarize in one sentence: BoundFlow schedules agentic workflows.")
        await ctx.run_agent(_agent())
        completed.append(True)
        return Complete()

    async with run_worker(worker):
        _, tenant = await create_isolated_tenant(cp, "cooldown-invoke")
        workflow = await cp.create_workflow("cooldown_invoke", tenant.id,
                                            config=WorkflowConfig(version=1))

        await cp.set_workflow_lifecycle_policy(workflow.id, _cooldown_policy())
        await cp.activate_workflow(workflow.id)

        # Invoke 1 — triggers cooldown rule.
        await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
        await wait_for_completion(cp, workflow.id)
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.COOLDOWN)

        # Invoke while in Cooldown — must be rejected immediately.
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
        assert exc_info.value.code() == grpc.StatusCode.FAILED_PRECONDITION

        # Wait for cooldown to expire and workflow to return to Active.
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.ACTIVE, timeout=60)

        # Invoke 2 — should now succeed.
        await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
        await wait_for_completion(cp, workflow.id)

        assert len(completed) == 2, f"Expected 2 completions, got {len(completed)}"
