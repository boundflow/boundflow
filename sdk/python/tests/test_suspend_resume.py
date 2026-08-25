"""End-to-end tests for SuspendWorkflow / ResumeWorkflow."""
from __future__ import annotations

import asyncio

import pytest

from boundflow import (
    BoundFlowWorker,
    Complete,
    FailedPreconditionError,
    LifecycleState,
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
    wait_for_workflow_state,
)


async def _suspended_workflow(cp, prefix: str, handler):
    """A workflow with one registered handler, activated and ready to invoke."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    worker.workflow(f"{prefix}_wf", version=1)(handler)
    tenant = await create_isolated_tenant(cp, prefix)
    workflow = await cp.create_workflow(f"{prefix}_wf", tenant.id, config=WorkflowConfig(version=1))
    await cp.activate_workflow(workflow.id)
    return worker, workflow


async def test_suspend_holds_queued_work_and_resume_releases_it(cp):
    """Suspending an idle workflow holds it immediately: invokes are refused while held,
    and the queued request is released and runs once resumed."""
    async def _entry(ctx):
        return Complete()

    worker, workflow = await _suspended_workflow(cp, "susp", _entry)

    async with run_worker(worker):
        first = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        await wait_for_completion(cp, first, timeout=60)

        suspension_id = await cp.suspend_workflow(workflow.id, reason="operator hold")
        assert suspension_id

        await wait_for_workflow_state(cp, workflow.id, WorkflowState.SUSPENDED)
        info = await cp.get_workflow(workflow.id)
        assert info.suspension is not None
        assert info.suspension.suspension_id == suspension_id
        assert info.suspension.reason == "operator hold"
        assert info.suspension.requested_at is not None

        # Nothing was running, so it drains immediately.
        for _ in range(60):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized"

        # A held workflow refuses new work outright.
        with pytest.raises(FailedPreconditionError):
            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)

        # Wrong id can't release it.
        with pytest.raises(FailedPreconditionError):
            await cp.resume_workflow(workflow.id, "not-the-right-id")

        await cp.resume_workflow(workflow.id, suspension_id)
        info = await cp.get_workflow(workflow.id)
        assert info.workflow_state == WorkflowState.ACTIVE
        assert info.suspension is None

        # ...and it runs again.
        second = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        run = await wait_for_completion(cp, second, timeout=60)
        assert run.status == RunStatus.COMPLETED


async def test_suspend_stop_current_cuts_the_running_run(cp):
    """stop_current_run cuts the in-flight run rather than draining it. The run is recorded
    as SUSPENDED — deliberately stopped, so it must not count as a workflow failure."""
    started = asyncio.Event()

    async def _entry(ctx):
        started.set()
        await asyncio.sleep(120)
        return Complete()

    worker, workflow = await _suspended_workflow(cp, "cut", _entry)

    async with run_worker(worker):
        request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        await asyncio.wait_for(started.wait(), timeout=30)
        await asyncio.sleep(2)  # let the server settle into its busy state

        suspension_id = await cp.suspend_workflow(
            workflow.id, reason="cut it", stop_current_run=True)

        run = await wait_for_completion(cp, request_id, timeout=90)
        assert run.run_outcome == RunOutcome.SUSPENDED, f"got {run.run_outcome}"

        for _ in range(120):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized"

        await cp.resume_workflow(workflow.id, suspension_id)
        assert (await cp.get_workflow(workflow.id)).workflow_state == WorkflowState.ACTIVE


async def test_suspend_drains_the_running_run_by_default(cp):
    """Without stop_current_run the in-flight run is left to finish: it completes normally
    (not SUSPENDED), and only then does the suspension finalize."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def _entry(ctx):
        started.set()
        await release.wait()
        return Complete()

    worker, workflow = await _suspended_workflow(cp, "drain", _entry)

    async with run_worker(worker):
        request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        await asyncio.wait_for(started.wait(), timeout=30)
        await asyncio.sleep(2)

        suspension_id = await cp.suspend_workflow(workflow.id, reason="drain it")
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.SUSPENDED)

        # Still draining: the run is untouched, so the suspension can't have finalized.
        info = await cp.get_workflow(workflow.id)
        assert info.suspension.finalized_at is None, "finalized while a run was still going"

        release.set()
        run = await wait_for_completion(cp, request_id, timeout=90)
        assert run.run_outcome == RunOutcome.SUCCESSFUL, f"drained run should succeed, got {run.run_outcome}"

        for _ in range(120):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized after the run drained"
        assert info.lifecycle_state == LifecycleState.HALTED

        await cp.resume_workflow(workflow.id, suspension_id)
        assert (await cp.get_workflow(workflow.id)).workflow_state == WorkflowState.ACTIVE
