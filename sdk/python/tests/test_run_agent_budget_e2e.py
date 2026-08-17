"""run_agent(budget=...) against a real backend — the shape Charter's loop needs:
several agent steps in one run, sharing one budget that BoundFlow's per-step policy
can't express on its own.
"""
from __future__ import annotations

from boundflow import (
    AgentDefinition,
    AgentPolicyLimitExceeded,
    BoundFlowWorker,
    Budget,
    Complete,
    MockLlmClient,
    RuntimePolicy,
    WorkflowConfig,
    turn,
)

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    run_worker,
    wait_for_completion,
)

AGENT = "responder"


def _never_submits():
    """Burns whatever cap it's given: always asks for a tool, never submits, so the
    forced submit_result is what ends each step."""
    return MockLlmClient(lambda _: turn(100, 20, "ping"))


def _agent() -> AgentDefinition:
    async def ping(_):
        return "pong"

    from boundflow import Tool
    return AgentDefinition(
        name=AGENT, system_prompt="mock", model="mock-model",
        tools=[Tool("ping", "ping", ping)],
        output_schema={"done": {"type": "boolean"}})


async def test_budget_spans_multiple_agent_steps_in_one_run(cp):
    """Server policy allows 5 calls per step; a 4-call budget shared across two steps
    must hold across both, not reset."""
    steps: list[int] = []

    worker = BoundFlowWorker(WORKER_ADDRESS, _never_submits())

    @worker.workflow("budget_wf", version=1)
    async def _entry(ctx):
        total, spent = 4, 0
        for _ in range(3):  # would run 3 steps x 5 calls = 15 without a budget
            try:
                result = await ctx.run_agent(_agent(), budget=Budget(max_llm_calls=total - spent))
            except AgentPolicyLimitExceeded:
                break  # budget gone — Charter's loop reports this rather than crashing
            spent += result.llm_calls_used
            steps.append(result.llm_calls_used)
        return Complete(result={"spent": spent})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "budget")
        wf = await cp.create_workflow("budget_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_runtime_policy(wf.id, AGENT, RuntimePolicy(max_llm_calls=5))
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)
            info = await wait_for_completion(cp, rid, timeout=60)

            assert info.status == "completed"
            assert sum(steps) == 4, f"budget of 4 must hold across steps, got {steps}"
            # First step capped by the budget (4), not the policy (5).
            assert steps[0] == 4, f"first step should take the whole budget, got {steps[0]}"

            metrics = await cp.get_workflow_metrics(wf.id)
            assert metrics.total_llm_calls == 4, \
                f"server should see exactly the budgeted calls, got {metrics.total_llm_calls}"
        finally:
            await cp.delete_workflow(wf.id)


async def test_policy_still_caps_a_generous_budget(cp):
    """The server-side policy stays the ceiling — a budget can't buy more than policy
    allows, since run_agent is called from workflow code."""
    used: list[int] = []
    worker = BoundFlowWorker(WORKER_ADDRESS, _never_submits())

    @worker.workflow("budget_ceiling_wf", version=1)
    async def _entry(ctx):
        result = await ctx.run_agent(_agent(), budget=Budget(max_llm_calls=99))
        used.append(result.llm_calls_used)
        return Complete(result={"calls": result.llm_calls_used})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "budget-ceiling")
        wf = await cp.create_workflow("budget_ceiling_wf", tenant.id,
                                      config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_runtime_policy(wf.id, AGENT, RuntimePolicy(max_llm_calls=2))
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)
            await wait_for_completion(cp, rid, timeout=60)
            assert used == [2], f"policy cap of 2 must win over a budget of 99, got {used}"
        finally:
            await cp.delete_workflow(wf.id)
