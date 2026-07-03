"""Port of PeriodicWorkflowTests.cs"""
from __future__ import annotations

import asyncio
import time

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

REPEAT_EVERY = 5
COOLDOWN_SECONDS = 8


def _agent():
    return AgentDefinition(
        name="analyst",
        system_prompt="You are a concise data analyst.",
        model=HAIKU,
        output_schema={"summary": {"type": "string"}},
    )


async def test_periodic_workflow_fires_automatically(cp, api_key):
    """A periodic workflow fires at least twice without any explicit invoke call."""
    firings: list[float] = []

    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("periodic_auto", version=1)
    async def _entry(ctx):
        ctx.add_context("task", "Summarize in one sentence: BoundFlow schedules agentic workflows.")
        await ctx.run_agent(_agent())
        firings.append(time.monotonic())
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "periodic-auto")
        workflow = await cp.create_workflow(
            "periodic_auto", tenant.id,
            config=WorkflowConfig(version=1, invoke_timeout_seconds=30,
                                  repeat_every_seconds=REPEAT_EVERY),
        )
        try:
            await cp.activate_workflow(workflow.id)

            # Wait for at least 2 automatic firings — no explicit invoke.
            deadline = asyncio.get_event_loop().time() + 240
            while len(firings) < 2:
                assert asyncio.get_event_loop().time() < deadline, "Timed out waiting for 2 periodic firings"
                await asyncio.sleep(0.5)

            gap = firings[1] - firings[0]
            assert gap >= REPEAT_EVERY, \
                f"Expected gap of at least {REPEAT_EVERY}s between firings, got {gap:.1f}s"
        finally:
            await cp.delete_workflow(workflow.id)


async def test_periodic_workflow_does_not_fire_during_cooldown(cp, api_key):
    """
    After the first firing triggers cooldown, the second firing must not happen
    until at least COOLDOWN_SECONDS after cooldown was entered.
    """
    firings: list[float] = []

    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("periodic_cooldown", version=1)
    async def _entry(ctx):
        firings.append(time.monotonic())
        ctx.add_context("task", "Summarize in one sentence: BoundFlow schedules agentic workflows.")
        await ctx.run_agent(_agent())
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "periodic-cooldown")
        workflow = await cp.create_workflow(
            "periodic_cooldown", tenant.id,
            config=WorkflowConfig(version=1, invoke_timeout_seconds=30,
                                  repeat_every_seconds=REPEAT_EVERY),
        )
        try:
            await cp.set_workflow_lifecycle_policy(workflow.id, [
                WorkflowRule(
                    metric=WorkflowMetric.NUM_LLM_CALLS,
                    threshold=1,
                    action=Cooldown(window=1, seconds=COOLDOWN_SECONDS),
                )
            ])
            await cp.activate_workflow(workflow.id)

            # Wait for first auto-firing and the resulting cooldown state.
            deadline = asyncio.get_event_loop().time() + 120
            while len(firings) < 1:
                assert asyncio.get_event_loop().time() < deadline, "Timed out waiting for first firing"
                await asyncio.sleep(0.5)
            # Periodic runs are auto-created — wait on the newest run.
            runs = await cp.list_workflow_runs(workflow.id)
            await wait_for_completion(cp, runs[0].request_id)
            await wait_for_workflow_state(cp, workflow.id, WorkflowState.COOLDOWN)
            cooldown_entered_at = time.monotonic()

            # Wait for cooldown to expire, then the second auto-firing.
            await wait_for_workflow_state(cp, workflow.id, WorkflowState.ACTIVE, timeout=60)
            deadline2 = asyncio.get_event_loop().time() + 120
            while len(firings) < 2:
                assert asyncio.get_event_loop().time() < deadline2, "Timed out waiting for second firing"
                await asyncio.sleep(0.5)

            cooldown_duration = firings[1] - cooldown_entered_at
            assert cooldown_duration >= COOLDOWN_SECONDS, \
                f"Expected second firing at least {COOLDOWN_SECONDS}s after cooldown start, " \
                f"got {cooldown_duration:.1f}s"
        finally:
            await cp.delete_workflow(workflow.id)
