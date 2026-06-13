"""Port of WorkflowLifecyclePolicyTests.cs"""
from __future__ import annotations

import grpc
import pytest

from boundflow import (
    AgentDefinition,
    ApprovalRequest,
    AwaitApproval,
    BoundFlowWorker,
    Complete,
    Cooldown,
    LifecycleState,
    MockLlmClient,
    Pause,
    SetVersion,
    WorkflowConfig,
    WorkflowMetric,
    WorkflowRule,
    WorkflowState,
    submit,
)
from boundflow.anthropic_client import AnthropicLlmClient

from .conftest import (
    HAIKU,
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
    wait_for_lifecycle_state,
    wait_for_workflow_state,
)


def _llm_agent():
    return AgentDefinition(
        name="analyst",
        system_prompt="You are a concise data analyst.",
        model=HAIKU,
        output_schema={"summary": {"type": "string"}},
    )


async def test_workflow_enters_cooldown_after_llm_call_threshold_exceeded(cp, api_key):
    """After one invocation making at least one LLM call, the cooldown rule fires."""
    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("cooldown_test", version=1)
    async def _entry(ctx):
        ctx.add_context("task", "Summarize in one sentence: BoundFlow schedules agentic workflows.")
        await ctx.run_agent(_llm_agent())
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "workflow-policy")
        workflow = await cp.create_workflow("cooldown_test", tenant.id,
                                            config=WorkflowConfig(version=1))
        try:
            await cp.set_workflow_lifecycle_policy(workflow.id, [
                WorkflowRule(
                    metric=WorkflowMetric.NUM_LLM_CALLS,
                    threshold=1,
                    action=Cooldown(window=1, seconds=10),
                )
            ])
            await cp.activate_workflow(workflow.id)

            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, workflow.id)

            state = await wait_for_workflow_state(cp, workflow.id, WorkflowState.COOLDOWN)
            assert state == WorkflowState.COOLDOWN
        finally:
            await cp.delete_workflow(workflow.id)


async def test_workflow_switches_to_new_version_after_llm_call_threshold_exceeded(cp, api_key):
    """
    Invoke 1 uses version 1 and makes an LLM call → set_version rule fires → version=2.
    Invoke 2 dispatches to the version 2 handler.
    """
    versions_run = []

    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("version_test", version=1)
    async def _v1(ctx):
        ctx.add_context("task", "Summarize in one sentence: BoundFlow schedules agentic workflows.")
        await ctx.run_agent(_llm_agent())
        versions_run.append(ctx.workflow_version)
        return Complete()

    @worker.workflow("version_test", version=2)
    async def _v2(ctx):
        versions_run.append(ctx.workflow_version)
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "version-rollback")
        workflow = await cp.create_workflow("version_test", tenant.id,
                                            config=WorkflowConfig(version=1))
        try:
            # Switch to version 2 once total LLM calls across all runs reaches 1.
            await cp.set_workflow_lifecycle_policy(workflow.id, [
                WorkflowRule(
                    metric=WorkflowMetric.NUM_LLM_CALLS,
                    threshold=1,
                    action=SetVersion(target=2),
                )
            ])
            await cp.activate_workflow(workflow.id)

            # Invoke 1 — runs version 1, makes an LLM call, rule fires → current_workflow_version=2.
            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, workflow.id)
            assert len(versions_run) >= 1 and versions_run[0] == 1, "Invoke 1 should have run version 1"

            # Invoke 2 — scheduler reads current_workflow_version=2.
            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, workflow.id)
            assert len(versions_run) >= 2 and versions_run[1] == 2, "Invoke 2 should have run version 2"
        finally:
            await cp.delete_workflow(workflow.id)


async def test_workflow_pauses_and_does_not_schedule_until_activated(cp, api_key):
    """
    After the pause rule fires, invoking is rejected with FAILED_PRECONDITION.
    After explicit activation, the next invocation completes normally.
    """
    completions = []
    invocation_count = [0]

    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("pause_test", version=1)
    async def _entry(ctx):
        invocation_count[0] += 1
        n = invocation_count[0]
        await ctx.run_agent(_llm_agent())
        completions.append(n)
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "workflow-pause")
        workflow = await cp.create_workflow("pause_test", tenant.id,
                                            config=WorkflowConfig(version=1))
        try:
            await cp.set_workflow_lifecycle_policy(workflow.id, [
                WorkflowRule(
                    metric=WorkflowMetric.NUM_LLM_CALLS,
                    threshold=1,
                    action=Pause(window=1),
                )
            ])
            await cp.activate_workflow(workflow.id)

            # Invoke 1 — pause rule fires on completion.
            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, workflow.id)
            await wait_for_workflow_state(cp, workflow.id, WorkflowState.PAUSED)

            # Invoke 2 — should be rejected immediately with FailedPrecondition.
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
            assert exc_info.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            # Activate — invoke should now succeed and complete.
            await cp.activate_workflow(workflow.id)
            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, workflow.id)

            assert 2 in completions, "Invoke 2 should have completed after activation"
        finally:
            await cp.delete_workflow(workflow.id)


async def test_workflow_enters_cooldown_after_customer_failure(cp):
    """ctx.mark_failed() increments num_failures; cooldown rule fires on threshold."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("failure_cooldown", version=1)
    async def _entry(ctx):
        ctx.mark_failed()
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "failure-cooldown")
        workflow = await cp.create_workflow("failure_cooldown", tenant.id,
                                            config=WorkflowConfig(version=1))
        try:
            await cp.set_workflow_lifecycle_policy(workflow.id, [
                WorkflowRule(
                    metric=WorkflowMetric.NUM_FAILURES,
                    threshold=1,
                    action=Cooldown(window=1, seconds=10),
                )
            ])
            await cp.activate_workflow(workflow.id)

            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, workflow.id)

            state = await wait_for_workflow_state(cp, workflow.id, WorkflowState.COOLDOWN)
            assert state == WorkflowState.COOLDOWN
        finally:
            await cp.delete_workflow(workflow.id)


async def test_workflow_pauses_after_approval_rejection(cp):
    """Rejecting the approval increments approval_rejections; pause rule fires."""
    captured: list[ApprovalRequest] = []

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("rejection_pause", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Complete(),
            on_reject=Complete(),
            timeout=60,
        )

    @worker.on_approval_requested
    async def _on_approval(request: ApprovalRequest):
        captured.append(request)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "rejection-pause")
        workflow = await cp.create_workflow("rejection_pause", tenant.id,
                                            config=WorkflowConfig(version=1))
        try:
            await cp.set_workflow_lifecycle_policy(workflow.id, [
                WorkflowRule(
                    metric=WorkflowMetric.APPROVAL_REJECTIONS,
                    threshold=1,
                    action=Pause(window=1),
                )
            ])

            await cp.activate_workflow(workflow.id)
            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)

            await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.AWAITING_APPROVAL)
            assert len(captured) == 1

            await cp.reject_workflow(workflow.id, captured[0].approval_id)

            state = await wait_for_workflow_state(cp, workflow.id, WorkflowState.PAUSED)
            assert state == WorkflowState.PAUSED
        finally:
            await cp.delete_workflow(workflow.id)


async def test_workflow_enters_cooldown_after_tool_failures(cp, api_key):
    """Tool handler exceptions are recorded; tool_failure_rate rule triggers cooldown."""
    import boundflow

    async def flaky_handler(_):
        raise RuntimeError("flaky tool failed")

    def agent():
        return AgentDefinition(
            name="flaky_agent",
            system_prompt=(
                "You are a test agent. You MUST call the `flaky` tool exactly once, "
                "then call submit_result regardless of the result."
            ),
            model=HAIKU,
            tools=[boundflow.Tool("flaky", "A tool that always fails. Call it once.", flaky_handler)],
            output_schema={"done": {"type": "boolean"}},
        )

    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("tool_failure_cooldown", version=1)
    async def _entry(ctx):
        await ctx.run_agent(agent())
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "tool-failure")
        workflow = await cp.create_workflow("tool_failure_cooldown", tenant.id,
                                            config=WorkflowConfig(version=1))
        try:
            await cp.set_workflow_lifecycle_policy(workflow.id, [
                WorkflowRule(
                    metric=WorkflowMetric.TOOL_FAILURE_RATE,
                    threshold=1,
                    action=Cooldown(window=1, seconds=10),
                    tool="flaky",
                )
            ])

            await cp.activate_workflow(workflow.id)
            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
            await wait_for_completion(cp, workflow.id)

            state = await wait_for_workflow_state(cp, workflow.id, WorkflowState.COOLDOWN)
            assert state == WorkflowState.COOLDOWN
        finally:
            await cp.delete_workflow(workflow.id)
