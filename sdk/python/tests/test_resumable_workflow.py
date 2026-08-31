"""A resumable workflow survives losing the worker that was running it.

Without `resumable`, a worker dying interrupts the workflow and a human has to
resolve it. With it, the job goes back on the queue and whoever picks it up next
carries on — at-least-once, which is why it is opt-in.
"""
from __future__ import annotations

import asyncio

import pytest

from boundflow import (
    BoundFlowWorker,
    Complete,
    LifecycleState,
    WorkflowConfig,
)
from tests.conftest import (
    create_isolated_tenant,
    dummy_mock,
    wait_for_completion,
    wait_for_lifecycle_state,
)

WORKFLOW = "resumable_pickup"


def _worker(started: asyncio.Event, finish: asyncio.Event, attempts: list):
    """A worker whose handler announces itself, then waits to be told to finish."""
    worker = BoundFlowWorker(llm=dummy_mock())

    @worker.workflow(WORKFLOW, version=1)
    async def entry(ctx):
        attempts.append(ctx._op.request_id)
        started.set()
        await finish.wait()
        return Complete(result={"attempts": len(attempts)})

    return worker


@pytest.mark.asyncio
async def test_a_resumable_run_continues_on_another_worker(cp):
    tenant = await create_isolated_tenant(cp, "resumable")
    wf = await cp.create_workflow(
        WORKFLOW, tenant.id, config=WorkflowConfig(version=1, resumable=True))
    await cp.activate_workflow(wf.id)

    attempts: list[str] = []

    # Worker A picks the operation up, then dies mid-run.
    started_a, finish_a = asyncio.Event(), asyncio.Event()
    task_a = asyncio.create_task(_worker(started_a, finish_a, attempts).run())
    request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=300)
    await asyncio.wait_for(started_a.wait(), timeout=60)
    task_a.cancel()
    await asyncio.gather(task_a, return_exceptions=True)

    # Worker B knows nothing about the task; it just claims whatever is available.
    started_b, finish_b = asyncio.Event(), asyncio.Event()
    task_b = asyncio.create_task(_worker(started_b, finish_b, attempts).run())
    try:
        await asyncio.wait_for(started_b.wait(), timeout=90)
        finish_b.set()
        info = await wait_for_completion(cp, request_id, timeout=90)
    finally:
        task_b.cancel()
        await asyncio.gather(task_b, return_exceptions=True)

    assert info.status == "completed", f"run did not complete: {info.status}"
    assert len(attempts) == 2, "the operation should have run twice — once per worker"
    assert attempts[0] == attempts[1] == request_id, "both attempts are the same run"


@pytest.mark.asyncio
async def test_without_resumable_the_workflow_is_interrupted(cp):
    """The default. Nothing picks the run up; a human has to."""
    tenant = await create_isolated_tenant(cp, "not-resumable")
    wf = await cp.create_workflow(WORKFLOW, tenant.id, config=WorkflowConfig(version=1))
    await cp.activate_workflow(wf.id)

    started, finish = asyncio.Event(), asyncio.Event()
    attempts: list[str] = []
    task = asyncio.create_task(_worker(started, finish, attempts).run())
    await cp.invoke_workflow(wf.id, operation_timeout_seconds=300)
    await asyncio.wait_for(started.wait(), timeout=60)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    await wait_for_lifecycle_state(cp, wf.id, LifecycleState.INTERRUPTED, timeout=90)
    assert len(attempts) == 1
