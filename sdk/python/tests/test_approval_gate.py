"""Port of ApprovalGateTests.cs"""
from __future__ import annotations

import pytest

from boundflow import (
    ApprovalRequest,
    AwaitApproval,
    BoundFlowWorker,
    Complete,
    LifecycleState,
    Next,
    WorkflowConfig,
)

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
    wait_for_lifecycle_state,
)


async def test_approve_gate_runs_approve_operation(cp):
    """Happy path: park → approve → on_approve operation runs."""
    captured: list[ApprovalRequest] = []
    approved_step_ran = [False]

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("approval_approve", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="approved_step", context=ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=60,
            justification="needs human sign-off",
        )

    @worker.operation("approval_approve", "approved_step")
    async def _approved(ctx):
        approved_step_ran[0] = True
        return Complete()

    @worker.on_approval_requested
    async def _on_approval(request: ApprovalRequest):
        captured.append(request)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "approval-approve")
        workflow = await cp.create_workflow("approval_approve", tenant.id,
                                            config=WorkflowConfig(version=1))

        await cp.activate_workflow(workflow.id)
        await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)

        await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.AWAITING_APPROVAL)

        assert len(captured) == 1
        req = captured[0]
        assert req.workflow_id == workflow.id
        assert req.justification == "needs human sign-off"
        assert req.approval_id

        await cp.approve_workflow(workflow.id, req.approval_id)
        await wait_for_completion(cp, workflow.id)

        assert approved_step_ran[0], "approved_step should have run after approval"


async def test_approval_timeout_runs_reject_operation(cp):
    """Approval gate auto-rejects once the timeout expires."""
    timed_out_ran = [False]
    approved_ran = [False]

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("approval_timeout", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="approved_step", context=ctx.context, timeout=30),
            on_reject=Next(operation="timed_out_step", context=ctx.context, timeout=30),
            timeout=8,
        )

    @worker.operation("approval_timeout", "approved_step")
    async def _approved(ctx):
        approved_ran[0] = True
        return Complete()

    @worker.operation("approval_timeout", "timed_out_step")
    async def _timed_out(ctx):
        timed_out_ran[0] = True
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "approval-timeout")
        workflow = await cp.create_workflow("approval_timeout", tenant.id,
                                            config=WorkflowConfig(version=1))

        await cp.activate_workflow(workflow.id)
        await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)

        # Wait for it to park, then let the timeout expire (don't approve or reject).
        await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.AWAITING_APPROVAL)
        await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.ACTIVE, timeout=30)

        assert timed_out_ran[0], "timed_out_step should have run after timeout"
        assert not approved_ran[0], "approved_step should NOT have run"


async def test_reject_gate_skips_approve_operation(cp):
    """Explicit rejection: on_reject branch runs, on_approve does not."""
    captured: list[ApprovalRequest] = []
    approved_ran = [False]

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("approval_reject", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="approved_step", context=ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=60,
        )

    @worker.operation("approval_reject", "approved_step")
    async def _approved(ctx):
        approved_ran[0] = True
        return Complete()

    @worker.on_approval_requested
    async def _on_approval(request: ApprovalRequest):
        captured.append(request)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "approval-reject")
        workflow = await cp.create_workflow("approval_reject", tenant.id,
                                            config=WorkflowConfig(version=1))

        await cp.activate_workflow(workflow.id)
        await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)

        await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.AWAITING_APPROVAL)
        assert len(captured) == 1

        await cp.reject_workflow(workflow.id, captured[0].approval_id)
        await wait_for_completion(cp, workflow.id)

        assert not approved_ran[0], "approved_step should NOT have run after rejection"
