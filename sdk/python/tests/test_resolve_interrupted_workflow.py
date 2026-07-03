"""End-to-end tests for ResolveInterruptedWorkflow and last_interrupted_request_id."""
from __future__ import annotations

import asyncio

import pytest

from boundflow import (
    BoundFlowWorker,
    Complete,
    FailedPreconditionError,
    LifecycleState,
    WorkflowConfig,
    WorkflowState,
)

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    wait_for_lifecycle_state,
)


async def _last_interrupted_request_id(cp, workflow_id: str) -> str:
    for w in await cp.list_workflows():
        if w.id == workflow_id:
            return w.last_interrupted_request_id
    raise AssertionError(f"workflow {workflow_id} not found in list_workflows")


async def test_resolve_interrupted_workflow_requires_matching_request_id(cp):
    """Losing the worker mid-operation is a platform failure: it interrupts the workflow
    (INTERRUPTED + DISABLED) and records last_interrupted_request_id. Resolution only
    succeeds when the caller passes that id, and re-activates the workflow.

    Note: a customer-domain failure (timeout, callback exception) would NOT interrupt —
    only a platform failure like this does."""
    started = asyncio.Event()
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("resolve_test", version=1)
    async def _entry(ctx):
        started.set()
        await asyncio.sleep(120)  # block so we can drop the worker while the op is in flight
        return Complete()

    tenant = await create_isolated_tenant(cp, "resolve")
    workflow = await cp.create_workflow("resolve_test", tenant.id, config=WorkflowConfig(version=1))
    await cp.activate_workflow(workflow.id)

    # Run the worker manually so we can kill it mid-operation (a real stream drop).
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)
    try:
        # High op timeout so the *stream drop* is the failure, not a timeout (which is soft).
        request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        await asyncio.wait_for(started.wait(), timeout=30)  # op is running on the worker
        await asyncio.sleep(2)  # let the server settle into its busy state (IN_PROGRESS acked)
    finally:
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

    # The lost worker interrupts the workflow.
    await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.INTERRUPTED)
    assert await cp.get_workflow_state(workflow.id) == WorkflowState.DISABLED

    # last_interrupted_request_id is exactly the request that was interrupted.
    assert await _last_interrupted_request_id(cp, workflow.id) == request_id

    # Resolving with a wrong id is rejected, and the workflow stays interrupted.
    with pytest.raises(FailedPreconditionError):
        await cp.resolve_interrupted_workflow(workflow.id, "not-the-right-id")
    assert await cp.get_workflow_lifecycle_state(workflow.id) == LifecycleState.INTERRUPTED

    # Resolving with the matching id re-activates the workflow.
    await cp.resolve_interrupted_workflow(workflow.id, request_id)
    assert await cp.get_workflow_lifecycle_state(workflow.id) == LifecycleState.ACTIVE
    assert await cp.get_workflow_state(workflow.id) == WorkflowState.ACTIVE
