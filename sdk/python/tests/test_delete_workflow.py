"""End-to-end tests for DeleteWorkflow's deletion_requested_at based flow.

An idle workflow (nothing in flight) finalizes to lifecycle_state DELETED
synchronously. A workflow with a run in flight must wait for that run to
finish; since finalization isn't hooked into CompleteRequest/FailRequest, it's
the periodic DeletionReconciler (partition sweep) that eventually notices and
finalizes it, so the deleted-mid-flight case has to poll past that interval.
"""
from __future__ import annotations

import asyncio

from boundflow import BoundFlowWorker, Complete, InvokeMode, WorkflowConfig

from .conftest import WORKER_ADDRESS, create_isolated_tenant, dummy_mock, run_worker


async def test_delete_idle_workflow_finalizes_immediately(cp):
    tenant = await create_isolated_tenant(cp, "del-idle")
    wf = await cp.create_workflow("del-idle-wf", tenant.id, config=WorkflowConfig(version=1))

    await cp.delete_workflow(wf.id)

    # Idle workflow: DeleteWorkflow finalizes synchronously, but the workflow
    # stays gettable (soft-deleted, not purged) until WorkflowPurgeReconciler
    # physically removes it later.
    info = await cp.get_workflow(wf.id)
    assert info.lifecycle_state.value == "deleted"


async def test_delete_mid_flight_stays_visible_then_reconciler_finalizes(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("del-mid-flight", version=1)
    async def _entry(ctx):
        await asyncio.sleep(3)
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "del-mid")
        wf = await cp.create_workflow("del-mid-flight", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)
        await cp.invoke_workflow(wf.id)

        # Give the worker a moment to actually pick up the run before deleting.
        await asyncio.sleep(1)

        await cp.delete_workflow(wf.id)

        # Still visible immediately after delete: the run hasn't finished, so
        # DeleteWorkflow couldn't finalize synchronously.
        info = await cp.get_workflow(wf.id)
        assert info.deletion_requested_at is not None
        assert info.lifecycle_state.value != "deleted"

        # The reconciler sweeps on a fixed interval (30s server-side); poll well
        # past that for it to notice the run finished and finalize the delete.
        # The workflow stays gettable throughout (soft-deleted, not purged) —
        # only lifecycle_state flips to deleted, it never 404s from this alone.
        deadline = asyncio.get_event_loop().time() + 60
        while True:
            info = await cp.get_workflow(wf.id)
            if info.lifecycle_state.value == "deleted":
                break
            assert asyncio.get_event_loop().time() < deadline, \
                "workflow was never finalized as deleted"
            await asyncio.sleep(2)


async def test_delete_abandons_unscheduled_requests(cp):
    # queue mode, no worker running: the first invoke occupies the single job
    # slot and never completes; later invokes stay unscheduled behind it.
    tenant = await create_isolated_tenant(cp, "del-abandon")
    wf = await cp.create_workflow(
        "del-abandon-wf", tenant.id,
        config=WorkflowConfig(version=1, invoke_mode=InvokeMode.QUEUE),
    )
    await cp.activate_workflow(wf.id)

    await cp.invoke_workflow(wf.id)
    queued_request_id = await cp.invoke_workflow(wf.id)

    await cp.delete_workflow(wf.id)

    info = await cp.get_request_info(queued_request_id)
    assert info.status == "abandoned"
