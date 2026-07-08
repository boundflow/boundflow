"""End-to-end tests for WorkflowConfig.invoke_mode.

  coalesce (default): a newer invoke supersedes older pending ones (latest-wins).
  queue:              every invoke runs, drained oldest-first (FIFO by version), one at
                      a time, bounded by max_queue_depth.

Runs hold the job slot briefly so concurrently-fired invokes pile up as the backlog
these tests exercise. No LLM — a mock client and a sleep-then-Complete handler.
"""
from __future__ import annotations

import asyncio

from boundflow import (
    BoundFlowWorker,
    Complete,
    RunStatus,
    WorkflowConfig,
)
from boundflow.errors import BoundflowError

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
)


async def _wait_all_terminal(cp, request_ids, timeout: int = 120):
    """Poll until every request is terminal (completed / failed / superceded)."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        infos = [await cp.get_request_info(r) for r in request_ids]
        if all(i.status.is_terminal() for i in infos):
            return infos
        assert asyncio.get_event_loop().time() < deadline, "timed out waiting for runs to drain"
        await asyncio.sleep(0.5)


def _slow_worker(name: str, hold: float = 0.4):
    """A worker whose one workflow holds the job slot for `hold` seconds then completes."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow(name, version=1)
    async def _entry(ctx):
        await asyncio.sleep(hold)
        return Complete()

    return worker


async def test_queue_runs_every_concurrent_invoke(cp):
    """queue: 5 invokes fired at once all run — none is superceded."""
    worker = _slow_worker("im_queue_all")
    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "im-queue-all")
        wf = await cp.create_workflow("im_queue_all", tenant.id,
                                      config=WorkflowConfig(version=1, invoke_mode="queue"))
        try:
            await cp.activate_workflow(wf.id)
            rids = await asyncio.gather(
                *[cp.invoke_workflow(wf.id, operation_timeout_seconds=30) for _ in range(5)])
            infos = await _wait_all_terminal(cp, rids)
            assert all(i.status == RunStatus.COMPLETED for i in infos), \
                [i.status.value for i in infos]
        finally:
            await cp.delete_workflow(wf.id)


async def test_coalesce_supersedes_older_invokes(cp):
    """coalesce (default): 5 invokes fired at once collapse to latest-wins — the newest
    runs, the rest are superceded."""
    worker = _slow_worker("im_coalesce")
    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "im-coalesce")
        wf = await cp.create_workflow("im_coalesce", tenant.id,
                                      config=WorkflowConfig(version=1))  # default coalesce
        try:
            await cp.activate_workflow(wf.id)
            rids = await asyncio.gather(
                *[cp.invoke_workflow(wf.id, operation_timeout_seconds=30) for _ in range(5)])
            infos = await _wait_all_terminal(cp, rids)
            statuses = [i.status for i in infos]
            completed = sum(s == RunStatus.COMPLETED for s in statuses)
            superceded = sum(s == RunStatus.SUPERCEDED for s in statuses)
            assert superceded >= 1, [s.value for s in statuses]      # older ones dropped
            assert completed < 5, [s.value for s in statuses]        # not all ran
            assert completed + superceded == 5
        finally:
            await cp.delete_workflow(wf.id)


async def test_queue_drains_in_fifo_order(cp):
    """queue: sequential invokes complete in the order they were made (FIFO by version)."""
    worker = _slow_worker("im_queue_fifo", hold=0.3)
    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "im-queue-fifo")
        wf = await cp.create_workflow("im_queue_fifo", tenant.id,
                                      config=WorkflowConfig(version=1, invoke_mode="queue"))
        try:
            await cp.activate_workflow(wf.id)
            rids = [await cp.invoke_workflow(wf.id, operation_timeout_seconds=30) for _ in range(5)]
            infos = await _wait_all_terminal(cp, rids)
            assert all(i.status == RunStatus.COMPLETED for i in infos)
            completed_at = [i.completed_at for i in infos]
            # invoked in order rids[0..4]; queue drains oldest-first, so completion times
            # must be non-decreasing in that same order.
            assert completed_at == sorted(completed_at), completed_at
        finally:
            await cp.delete_workflow(wf.id)


async def test_queue_backpressure_rejects_at_max_depth(cp):
    """queue: once the backlog reaches max_queue_depth, further invokes are rejected while
    the queued ones keep their place."""
    worker = _slow_worker("im_backpressure", hold=3.0)  # hold the slot through the burst
    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "im-backpressure")
        wf = await cp.create_workflow("im_backpressure", tenant.id,
                                      config=WorkflowConfig(version=1, invoke_mode="queue", max_queue_depth=2))
        try:
            await cp.activate_workflow(wf.id)
            accepted, rejected, first_err = 0, 0, ""
            for _ in range(8):
                try:
                    await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
                    accepted += 1
                except BoundflowError as e:
                    rejected += 1
                    first_err = first_err or str(e)
            assert rejected >= 1, f"expected rejections, got accepted={accepted}"
            # at most the running one + max_queue_depth queued may be admitted
            assert accepted <= 3, f"admitted more than running+cap: {accepted}"
            assert "queue" in first_err.lower()
        finally:
            await cp.delete_workflow(wf.id)
