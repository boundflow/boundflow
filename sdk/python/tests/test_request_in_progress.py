"""A run's status while it is actually running.

`scheduled` used to cover a run's whole life — queued, executing, parked at a gate —
because nothing advanced it after the scheduler created the job. `in_progress` was in
the enum, in the domain type and in RunStatus, and never written by anything.

The interesting case is the last test: the direct write from the worker and the
sweep behind it both fire while a run is in flight, so both have to refuse to touch a
request that has already finished.
"""
from __future__ import annotations

import asyncio

import pytest

from boundflow import (
    AwaitApproval,
    BoundFlowWorker,
    Complete,
    LifecycleState,
    Next,
    RunStatus,
    WorkflowConfig,
)
from tests.conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    wait_for_lifecycle_state,
)


async def _wait_status(cp, request_id, expected, timeout=30):
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while True:
        last = (await cp.get_request_info(request_id)).status
        if last == expected:
            return
        assert asyncio.get_event_loop().time() < deadline, \
            f"timed out waiting for {expected}, last saw {last}"
        await asyncio.sleep(0.25)


@pytest.mark.asyncio
async def test_a_running_request_says_in_progress(cp):
    started, release = asyncio.Event(), asyncio.Event()
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    wtype = "req_in_progress"

    @worker.workflow(wtype, version=1)
    async def _entry(ctx):
        started.set()
        await release.wait()
        return Complete()

    task = asyncio.create_task(worker.run())
    try:
        tenant = await create_isolated_tenant(cp, "in-progress")
        wf = await cp.create_workflow(wtype, tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)
        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=120)

        await asyncio.wait_for(started.wait(), timeout=60)
        await _wait_status(cp, request_id, RunStatus.IN_PROGRESS)

        release.set()
        await _wait_status(cp, request_id, RunStatus.COMPLETED)
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_run_parked_at_a_gate_is_still_in_progress(cp):
    """A parked run is waiting on a person, not queued. Which gate it is parked at
    stays on the workflow's lifecycle_state — the run only says it hasn't finished."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    wtype = "req_in_progress_gate"

    @worker.workflow(wtype, version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="after", context=ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=300,
            justification="hold here",
        )

    @worker.operation(wtype, "after")
    async def _after(ctx):
        return Complete()

    task = asyncio.create_task(worker.run())
    try:
        tenant = await create_isolated_tenant(cp, "in-progress-gate")
        wf = await cp.create_workflow(wtype, tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)
        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=300)

        await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL)
        info = await cp.get_request_info(request_id)
        assert info.status == RunStatus.IN_PROGRESS, \
            f"a run parked at a gate should not read as queued, got {info.status}"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_finished_request_is_never_dragged_back(cp):
    """The sweep runs every tick and the worker writes directly on every claim, both
    while runs are completing around them. Neither may move a terminal request."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    wtype = "req_in_progress_terminal"

    @worker.workflow(wtype, version=1)
    async def _entry(ctx):
        return Complete(result={"ok": True})

    task = asyncio.create_task(worker.run())
    try:
        tenant = await create_isolated_tenant(cp, "in-progress-terminal")
        wf = await cp.create_workflow(wtype, tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)

        # One at a time: coalesce mode is latest-wins, so overlapping invokes would
        # supersede each other rather than all completing.
        ids = []
        for _ in range(3):
            request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)
            await _wait_status(cp, request_id, RunStatus.COMPLETED, timeout=60)
            ids.append(request_id)

        # Long enough for several scheduler ticks to sweep over these rows.
        await asyncio.sleep(5)
        for request_id in ids:
            info = await cp.get_request_info(request_id)
            assert info.status == RunStatus.COMPLETED, \
                f"{request_id} moved off completed to {info.status}"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
