"""A suspension must not hang on a run no worker can reach."""
from __future__ import annotations

import asyncio

from boundflow import (
    AwaitApproval,
    BoundFlowWorker,
    Complete,
    LifecycleState,
    Next,
    Next,
    RunOutcome,
    RunStatus,
    WorkflowConfig,
    WorkflowState,
)

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
    wait_for_lifecycle_state,
    wait_for_workflow_state,
)


async def test_stop_current_finishes_a_run_parked_at_an_approval_gate(cp):
    """A parked job has no worker and can never be acquired by one (AcquireJob excludes the
    awaiting states), so nothing could honour stop_current_run and the suspension waited
    forever on an approval that was never coming. The server finishes it instead."""
    approved_step_ran = [False]

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("susp_gate", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="approved_step", context=ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=600,  # long, so the gate can't resolve itself by timing out
            justification="needs human sign-off",
        )

    @worker.operation("susp_gate", "approved_step")
    async def _approved(ctx):
        approved_step_ran[0] = True
        return Complete()

    tenant = await create_isolated_tenant(cp, "susp-gate")
    workflow = await cp.create_workflow("susp_gate", tenant.id, config=WorkflowConfig(version=1))
    await cp.activate_workflow(workflow.id)

    async with run_worker(worker):
        request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.AWAITING_APPROVAL)

        suspension_id = await cp.suspend_workflow(
            workflow.id, reason="stop it, gate and all", stop_current_run=True)
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.SUSPENDED)

        # The parked run is finished server-side, recorded as stopped rather than failed.
        run = await wait_for_completion(cp, request_id, timeout=120)
        assert run.run_outcome == RunOutcome.SUSPENDED, f"got {run.run_outcome}"
        assert not approved_step_ran[0], "the approve branch must not have run"

        # ...and only then can the suspension finalize, which is what used to hang.
        for _ in range(240):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized"

        await cp.resume_workflow(workflow.id, suspension_id)
        assert (await cp.get_workflow(workflow.id)).workflow_state == WorkflowState.ACTIVE


async def test_stop_current_finishes_a_run_waiting_out_a_next_delay(cp):
    """`Next(delay_seconds=...)` puts dispatch_at in the future, so for the length of the
    delay nothing can acquire the job to honour the flag. Same hang as a gate, on a timer."""
    later_ran = [False]
    first_done = asyncio.Event()

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("susp_delay", version=1)
    async def _entry(ctx):
        first_done.set()
        return Next(operation="later", context=ctx.context, timeout=30, delay_seconds=600)

    @worker.operation("susp_delay", "later")
    async def _later(ctx):
        later_ran[0] = True
        return Complete()

    tenant = await create_isolated_tenant(cp, "susp-delay")
    workflow = await cp.create_workflow("susp_delay", tenant.id, config=WorkflowConfig(version=1))
    await cp.activate_workflow(workflow.id)

    async with run_worker(worker):
        request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        # Must actually finish, or the worker still holds the job and the lease-renewal
        # path honours the suspension instead — which would pass without the fix.
        await asyncio.wait_for(first_done.wait(), timeout=60)
        await asyncio.sleep(5)

        suspension_id = await cp.suspend_workflow(
            workflow.id, reason="don't wait ten minutes", stop_current_run=True)
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.SUSPENDED)

        run = await wait_for_completion(cp, request_id, timeout=120)
        assert run.run_outcome == RunOutcome.SUSPENDED, f"got {run.run_outcome}"
        assert not later_ran[0], "the delayed operation must not have run"

        for _ in range(240):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized"

        await cp.resume_workflow(workflow.id, suspension_id)
        assert (await cp.get_workflow(workflow.id)).workflow_state == WorkflowState.ACTIVE
