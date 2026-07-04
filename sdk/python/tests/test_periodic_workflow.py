"""Port of PeriodicWorkflowTests.cs"""
from __future__ import annotations

import asyncio
import time

import pytest

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Cooldown,
    Complete,
    InvalidArgumentError,
    MockLlmClient,
    Tool,
    WorkflowConfig,
    WorkflowMetric,
    WorkflowRule,
    WorkflowState,
    submit,
    turn,
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

REPEAT_EVERY = 30   # the scheduler polls periodic workflows every 30s; that's the floor
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


async def test_repeat_every_below_the_minimum_is_rejected(cp):
    """A repeat interval below the scheduler's poll cadence can't be honored, so
    creation rejects it outright instead of silently rounding up (see PeriodicPollSeconds)."""
    tenant = await create_isolated_tenant(cp, "periodic-min")

    with pytest.raises(InvalidArgumentError):
        await cp.create_workflow(
            "too_fast", tenant.id,
            config=WorkflowConfig(version=1, repeat_every_seconds=REPEAT_EVERY - 1))

    # The floor itself is allowed, and 0 (no repeat) is always allowed.
    at_floor = await cp.create_workflow(
        "at_floor", tenant.id,
        config=WorkflowConfig(version=1, repeat_every_seconds=REPEAT_EVERY))
    assert at_floor.config.repeat_every_seconds == REPEAT_EVERY

    no_repeat = await cp.create_workflow(
        "no_repeat", tenant.id, config=WorkflowConfig(version=1, repeat_every_seconds=0))
    assert no_repeat.config.repeat_every_seconds == 0


async def test_periodic_run_accrues_cost_so_cost_policies_fire(cp):
    """Regression: periodic runs must resolve model pricing so their token usage costs
    money. Without it a periodic run prices to $0, so cost-based lifecycle policies (here
    a cost -> cooldown) would silently never fire — the workflow would run past its budget
    forever. No Anthropic key: a mock burns the tokens."""
    async def _noop(_):
        return "ok"

    def _burn(ctx):
        # 60k input tokens on Haiku = $0.06/run, over the $0.05 threshold below.
        return turn(60_000, 0, "noop") if ctx.turn_index == 0 else submit()

    worker = BoundFlowWorker(WORKER_ADDRESS, MockLlmClient(_burn))

    @worker.workflow("costly_periodic", version=1)
    async def _entry(ctx):
        await ctx.run_agent(AgentDefinition(
            name="spender", system_prompt="burn tokens", model=HAIKU,
            tools=[Tool("noop", "A no-op step.", _noop)],
            output_schema={"summary": {"type": "string"}}))
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "periodic-cost")
        wf = await cp.create_workflow(
            "costly_periodic", tenant.id,
            config=WorkflowConfig(version=1, invoke_timeout_seconds=30,
                                  repeat_every_seconds=REPEAT_EVERY))
        try:
            await cp.set_workflow_lifecycle_policy(wf.id, [
                WorkflowRule(metric=WorkflowMetric.COST, threshold=0.05,
                             action=Cooldown(window=1, seconds=COOLDOWN_SECONDS)),
            ])
            await cp.activate_workflow(wf.id)
            # One periodic run spends $0.06 > $0.05, so the cost rule must fire and cool
            # the workflow down. If pricing weren't resolved for periodic runs, its cost
            # would be $0 and this would time out.
            await wait_for_workflow_state(cp, wf.id, WorkflowState.COOLDOWN, timeout=120)
        finally:
            await cp.delete_workflow(wf.id)
