"""End-to-end test for WorkflowPurgeReconciler.

A deleted workflow finalizes to lifecycle_state DELETED (see
test_delete_workflow.py) but stays gettable — its row isn't actually removed
until WorkflowPurgeReconciler notices it's past BOUNDFLOW_WORKFLOW_PURGE_AGE_SECONDS
and purges it. That threshold is shortened in CI (and must be shortened locally
too) for this test to complete in reasonable time; against the production
default (1h) this test would just time out.
"""
from __future__ import annotations

import asyncio

from boundflow import NotFoundError, WorkflowConfig

from .conftest import create_isolated_tenant


async def test_deleted_workflow_is_purged_after_age_threshold(cp):
    tenant = await create_isolated_tenant(cp, "purge-wf")
    wf = await cp.create_workflow("purge-wf-wf", tenant.id, config=WorkflowConfig(version=1))

    await cp.delete_workflow(wf.id)

    # Finalizes synchronously (idle workflow) but the row persists until purged.
    info = await cp.get_workflow(wf.id)
    assert info.lifecycle_state.value == "deleted"

    # WorkflowPurgeReconciler sweeps on a fixed interval (30s server-side) and
    # only purges rows past the age threshold; poll well past both.
    deadline = asyncio.get_event_loop().time() + 120
    while True:
        try:
            await cp.get_workflow(wf.id)
        except NotFoundError:
            break
        assert asyncio.get_event_loop().time() < deadline, \
            "workflow was never purged"
        await asyncio.sleep(3)
