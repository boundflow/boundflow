"""End-to-end test for TenantPurgeReconciler.

DeleteTenant's inline best-effort purge only succeeds if the workflows table
is already empty for that tenant. A workflow that's been soft-deleted but not
yet physically purged (see test_workflow_purge_reconciler.py) still blocks
that inline attempt, so the tenant stays soft-deleted until
WorkflowPurgeReconciler clears the workflow row and TenantPurgeReconciler's
own sweep then notices and purges the tenant. Requires
BOUNDFLOW_WORKFLOW_PURGE_AGE_SECONDS to be shortened (as in CI) to complete
in reasonable time.
"""
from __future__ import annotations

import asyncio

from boundflow import NotFoundError, WorkflowConfig

from .conftest import create_isolated_tenant


async def test_deleted_tenant_is_purged_once_workflows_are_gone(cp):
    tenant = await create_isolated_tenant(cp, "purge-tenant")
    wf = await cp.create_workflow("purge-tenant-wf", tenant.id, config=WorkflowConfig(version=1))
    await cp.delete_workflow(wf.id)

    # Idle workflow finalizes immediately (workflow_count back to 0), so the
    # tenant's soft-delete guard passes even though the workflow row persists.
    await cp.delete_tenant(tenant.id)

    # The inline best-effort purge in DeleteTenant can't have succeeded yet —
    # the workflow row is still there — so the tenant should still be visible,
    # soft-deleted.
    info = await cp.get_tenant(tenant.id)
    assert info.deleted_at is not None

    # WorkflowPurgeReconciler has to clear the workflow row first, then
    # TenantPurgeReconciler's own sweep has to notice and purge the tenant.
    # Both run on a fixed 30s interval, so this can take a couple of sweeps.
    deadline = asyncio.get_event_loop().time() + 180
    while True:
        try:
            await cp.get_tenant(tenant.id)
        except NotFoundError:
            break
        assert asyncio.get_event_loop().time() < deadline, \
            "tenant was never purged"
        await asyncio.sleep(3)
