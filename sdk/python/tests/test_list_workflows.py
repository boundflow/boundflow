"""ListWorkflows — the read-only observability primitive.

Pure control-plane (no LLM/worker needed): create workflows, then assert they
show up in list_workflows() with the exact type/state we expect, and that the
list reflects a real state transition. All mock-safe.
"""
from __future__ import annotations

from boundflow import LifecycleState, WorkflowConfig, WorkflowState
from .conftest import create_isolated_tenant


async def test_list_workflows_returns_created_workflows_with_expected_state(cp):
    tenant = await create_isolated_tenant(cp, "list")
    wf_a = await cp.create_workflow("alpha", tenant.id, config=WorkflowConfig(version=1))
    wf_b = await cp.create_workflow("beta", tenant.id, config=WorkflowConfig(version=2))

    by_id = {s.id: s for s in await cp.list_workflows()}

    assert wf_a.id in by_id, "created workflow should appear in the list"
    assert wf_b.id in by_id

    a = by_id[wf_a.id]
    assert a.workflow_type == "alpha"
    assert a.tenant_id == tenant.id
    assert a.version == 1
    # A freshly created workflow is ACTIVE in its lifecycle but PAUSED for dispatch
    # ("created and paused"): assert the exact states, not just truthiness.
    assert a.lifecycle_state == LifecycleState.ACTIVE
    assert a.workflow_state == WorkflowState.PAUSED

    b = by_id[wf_b.id]
    assert b.workflow_type == "beta"
    assert b.version == 2


async def test_list_workflows_reflects_state_changes(cp):
    # The list must show *live* state — activating a workflow should change what
    # the list reports, or it isn't real observability.
    tenant = await create_isolated_tenant(cp, "list-state")
    wf = await cp.create_workflow("svc", tenant.id, config=WorkflowConfig(version=1))

    before = {s.id: s for s in await cp.list_workflows()}[wf.id]
    assert before.workflow_state == WorkflowState.PAUSED

    await cp.activate_workflow(wf.id)

    after = {s.id: s for s in await cp.list_workflows()}[wf.id]
    assert after.workflow_state == WorkflowState.ACTIVE
    assert after.lifecycle_state == LifecycleState.ACTIVE


async def test_list_workflows_spans_tenants_in_the_group(cp):
    # Both tenants live under the caller's tenant group (the API key), so both
    # tenants' workflows are visible — listing is group-scoped, not tenant-scoped.
    t1 = await create_isolated_tenant(cp, "list-a")
    t2 = await create_isolated_tenant(cp, "list-b")
    wf1 = await cp.create_workflow("svc1", t1.id, config=WorkflowConfig(version=1))
    wf2 = await cp.create_workflow("svc2", t2.id, config=WorkflowConfig(version=1))

    ids = {s.id for s in await cp.list_workflows()}
    assert {wf1.id, wf2.id} <= ids
