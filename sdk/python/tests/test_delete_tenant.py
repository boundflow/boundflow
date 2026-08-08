"""End-to-end tests for DeleteTenant's soft-delete + best-effort purge flow."""
from __future__ import annotations

import pytest

from boundflow import FailedPreconditionError, NotFoundError, WorkflowConfig

from .conftest import create_isolated_tenant


async def test_delete_empty_tenant_purges_immediately(cp):
    tenant = await create_isolated_tenant(cp, "del-tenant-empty")

    await cp.delete_tenant(tenant.id)

    # No workflows ever existed, so the inline best-effort purge in DeleteTenant
    # succeeds synchronously — the tenant is fully gone by the time the call returns.
    with pytest.raises(NotFoundError):
        await cp.get_tenant(tenant.id)


async def test_delete_tenant_with_live_workflow_fails(cp):
    tenant = await create_isolated_tenant(cp, "del-tenant-live-wf")
    await cp.create_workflow("del-tenant-live-wf-wf", tenant.id, config=WorkflowConfig(version=1))

    with pytest.raises(FailedPreconditionError):
        await cp.delete_tenant(tenant.id)

    # Refused, so the tenant is still there and still live.
    info = await cp.get_tenant(tenant.id)
    assert info.deleted_at is None
