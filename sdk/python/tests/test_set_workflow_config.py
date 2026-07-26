"""SetWorkflowConfig — replaces a workflow's config wholesale, including its
current version.

Pure control-plane (no LLM/worker): create a workflow, update its config, and
assert the response reflects every field, that get_workflow agrees afterward,
and that it rejects a too-small repeat interval.
"""
from __future__ import annotations

import pytest

from boundflow import InvalidArgumentError, InvokeMode, Workflow, WorkflowConfig
from .conftest import create_isolated_tenant


async def test_set_workflow_config_returns_updated_workflow(cp):
    tenant = await create_isolated_tenant(cp, "set-config")
    wf = await cp.create_workflow("checkout", tenant.id, config=WorkflowConfig(version=1))

    new_config = WorkflowConfig(
        version=2, invoke_timeout_seconds=45, repeat_every_seconds=30,
        triggerable=False, invoke_mode=InvokeMode.QUEUE, max_queue_depth=10,
    )
    updated = await cp.set_workflow_config(wf.id, new_config)

    assert isinstance(updated, Workflow)
    assert updated.id == wf.id
    assert updated.config.version == 2
    assert updated.config.invoke_timeout_seconds == 45
    assert updated.config.repeat_every_seconds == 30
    assert updated.config.triggerable is False
    assert updated.config.invoke_mode == InvokeMode.QUEUE
    assert updated.config.max_queue_depth == 10


async def test_set_workflow_config_reflected_by_get_workflow(cp):
    tenant = await create_isolated_tenant(cp, "set-config-get")
    wf = await cp.create_workflow("checkout", tenant.id, config=WorkflowConfig(version=1))

    await cp.set_workflow_config(wf.id, WorkflowConfig(version=3))

    info = await cp.get_workflow(wf.id)
    assert info.version == 3


async def test_set_workflow_config_rejects_repeat_below_minimum(cp):
    tenant = await create_isolated_tenant(cp, "set-config-repeat")
    wf = await cp.create_workflow("checkout", tenant.id, config=WorkflowConfig(version=1))

    with pytest.raises(InvalidArgumentError):
        await cp.set_workflow_config(wf.id, WorkflowConfig(version=1, repeat_every_seconds=1))
