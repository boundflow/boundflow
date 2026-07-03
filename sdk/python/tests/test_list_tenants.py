"""End-to-end test for ListTenants."""
from __future__ import annotations

import uuid


async def test_list_tenants_returns_the_callers_tenants(cp):
    """ListTenants returns the tenants in the caller's tenant group. We create a couple of
    uniquely-named tenants and assert both come back (other tenants from other runs may share
    the group, so we check for presence, not an exact set)."""
    uid = uuid.uuid4().hex[:8]
    a = await cp.create_tenant(f"list-tenants-a-{uid}")
    b = await cp.create_tenant(f"list-tenants-b-{uid}")

    tenants = await cp.list_tenants()
    by_id = {t.id: t for t in tenants}

    assert a.id in by_id, f"tenant {a.id} missing from list_tenants"
    assert b.id in by_id, f"tenant {b.id} missing from list_tenants"
    assert by_id[a.id].name == f"list-tenants-a-{uid}"
    # All tenants are scoped to the caller's group.
    assert by_id[a.id].tenant_group_id == a.tenant_group_id
    assert by_id[b.id].tenant_group_id == b.tenant_group_id
