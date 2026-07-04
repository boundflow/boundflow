"""GetWorkflow — the single-resource read (current version + live state).

Pure control-plane (no LLM/worker): create a workflow, then assert get_workflow
returns it with the exact version/type/state, reflects a live state change, and
raises NotFoundError on an unknown id.
"""
from __future__ import annotations

import pytest

from boundflow import (
    LifecycleState, NotFoundError, WorkflowConfig, WorkflowInfo, WorkflowState,
)
from .conftest import create_isolated_tenant


async def test_get_workflow_returns_current_version_and_state(cp):
    tenant = await create_isolated_tenant(cp, "get")
    wf = await cp.create_workflow("checkout", tenant.id, config=WorkflowConfig(version=3))

    info = await cp.get_workflow(wf.id)

    assert isinstance(info, WorkflowInfo)
    assert info.id == wf.id
    assert info.workflow_type == "checkout"
    assert info.tenant_id == tenant.id
    assert info.version == 3
    # Freshly created: ACTIVE lifecycle, PAUSED for dispatch ("created and paused").
    assert info.lifecycle_state == LifecycleState.ACTIVE
    assert info.workflow_state == WorkflowState.PAUSED


async def test_get_workflow_reflects_state_change(cp):
    # get_workflow must report *live* state — activating changes what it returns.
    tenant = await create_isolated_tenant(cp, "get-state")
    wf = await cp.create_workflow("svc", tenant.id, config=WorkflowConfig(version=1))

    assert (await cp.get_workflow(wf.id)).workflow_state == WorkflowState.PAUSED

    await cp.activate_workflow(wf.id)

    after = await cp.get_workflow(wf.id)
    assert after.workflow_state == WorkflowState.ACTIVE
    assert after.lifecycle_state == LifecycleState.ACTIVE


async def test_get_workflow_unknown_id_raises_not_found(cp):
    with pytest.raises(NotFoundError):
        await cp.get_workflow("00000000-0000-0000-0000-000000000000")
