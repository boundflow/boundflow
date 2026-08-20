"""A run abandoned before its worker acked is still recovered.

The server sends the operation and waits for an IN_PROGRESS ack. If the stream
drops in that window it deliberately does *not* fail the run — it can't know
whether the client ever started, so it leaves the job for the orphan sweep, the
same reconciliation every stream-scoped write in rpcworker relies on
("stream drop doesn't cancel it; the sweep reconciles if lost").

The session's teardown then releases the lease, which clears `lease_expires_at`
— the signal that sweep waits on. Nulling it is right for a claimable job, which
becomes instantly available, and wrong for one still marked `dispatched`, which
becomes invisible: unclaimable (AcquireJob skips running statuses) and unsweepable.
The workflow reports `invoking` forever.
"""
from __future__ import annotations

import asyncio

import pytest

from boundflow import BoundFlowWorker, Complete, LifecycleState, WorkflowConfig
from boundflow import _transport
from tests.conftest import (
    create_isolated_tenant,
    dummy_mock,
    wait_for_lifecycle_state,
)

WORKFLOW = "orphaned_recovery"


def _silence_the_ack(monkeypatch, launched: asyncio.Event):
    """Make the worker drop its IN_PROGRESS ack, so the server is still waiting for
    one when the stream goes away. Everything else it sends goes through."""
    from boundflow.v1 import operation_pb2 as op_pb

    original = _transport.WorkerSession._write

    async def write(self, call, msg):
        update = msg.update if msg.HasField("update") else None
        if update is not None and update.result.status == op_pb.OPERATION_STATUS_IN_PROGRESS:
            launched.set()
            return
        return await original(self, call, msg)

    monkeypatch.setattr(_transport.WorkerSession, "_write", write)


@pytest.mark.asyncio
async def test_a_run_abandoned_before_its_ack_is_recovered(cp, monkeypatch):
    launched = asyncio.Event()
    _silence_the_ack(monkeypatch, launched)

    worker = BoundFlowWorker(llm=dummy_mock())

    @worker.workflow(WORKFLOW, version=1)
    async def entry(ctx):
        await asyncio.sleep(600)
        return Complete()

    tenant = await create_isolated_tenant(cp, "orphaned")
    wf = await cp.create_workflow(WORKFLOW, tenant.id, config=WorkflowConfig(version=1))
    await cp.activate_workflow(wf.id)

    task = asyncio.create_task(worker.run())
    try:
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=600)
        # The operation has been sent and its ack swallowed: the server is now in
        # ConnectedWaiting with the job marked dispatched.
        await asyncio.wait_for(launched.wait(), timeout=60)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    # Without the fix this never arrives. The job sits in `dispatched` with no owner
    # and no lease, and every sweeper reads it as work someone is doing.
    await wait_for_lifecycle_state(cp, wf.id, LifecycleState.INTERRUPTED, timeout=180)
