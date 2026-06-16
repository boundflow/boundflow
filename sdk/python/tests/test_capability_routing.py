"""
Verifies that workers only receive jobs matching their registered (resource_type, workflow_version).

Worker v2 runs alone first — without capability filtering it would pick up the v1 job
(no handler → FAILED). With filtering it correctly ignores it. Worker v1 then starts
and picks up the v1 job, both workflows complete successfully.
"""
from __future__ import annotations

import asyncio

from boundflow import BoundFlowWorker, Complete, WorkflowConfig
from boundflow.control_plane import LifecycleState

from .conftest import WORKER_ADDRESS, create_isolated_tenant, dummy_mock, run_worker, wait_for_completion


async def test_workers_only_receive_jobs_for_registered_version(cp, boundflow_api_key):
    v1_handled = []
    v2_handled = []

    worker_v1 = BoundFlowWorker(WORKER_ADDRESS, dummy_mock(), api_key=boundflow_api_key)
    worker_v2 = BoundFlowWorker(WORKER_ADDRESS, dummy_mock(), api_key=boundflow_api_key)

    @worker_v1.workflow("cap_workflow", version=1)
    async def handle_v1(ctx):
        v1_handled.append(True)
        return Complete()

    @worker_v2.workflow("cap_workflow", version=2)
    async def handle_v2(ctx):
        v2_handled.append(True)
        return Complete()

    tenant = await create_isolated_tenant(cp, "cap-routing")
    wf1 = await cp.create_workflow("cap_workflow", tenant.id, config=WorkflowConfig(version=1))
    wf2 = await cp.create_workflow("cap_workflow", tenant.id, config=WorkflowConfig(version=2))

    try:
        await cp.activate_workflow(wf1.id)
        await cp.activate_workflow(wf2.id)
        await cp.invoke_workflow(wf1.id, operation_timeout_seconds=30)
        await cp.invoke_workflow(wf2.id, operation_timeout_seconds=30)

        # Run Worker v2 alone first — without capability filtering it would pick up
        # the v1 job immediately (no handler → FAILED). With filtering it ignores it.
        async with run_worker(worker_v2):
            await asyncio.sleep(3)

        # Now start Worker v1 — with new code the v1 job is still pending and gets
        # picked up here. With old code it was already FAILED by Worker v2.
        async with run_worker(worker_v1):
            state1 = await wait_for_completion(cp, wf1.id)
            state2 = await wait_for_completion(cp, wf2.id)

        assert state1 == LifecycleState.ACTIVE, f"v1 workflow ended in {state1} — wrong worker may have picked it up"
        assert state2 == LifecycleState.ACTIVE, f"v2 workflow ended in {state2}"
        assert len(v1_handled) == 1, f"v1 handler ran {len(v1_handled)} times, expected 1"
        assert len(v2_handled) == 1, f"v2 handler ran {len(v2_handled)} times, expected 1"
    finally:
        await cp.delete_workflow(wf1.id)
        await cp.delete_workflow(wf2.id)
