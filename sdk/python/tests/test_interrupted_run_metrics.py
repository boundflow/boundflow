"""What a run spent survives the worker that was running it.

End to end, because the two halves only meet on the wire: the SDK reports metrics
while the operation is still going, and the server promotes them when the run turns
out never to finish. Either half missing and an interrupted run's spend is lost —
silently, which is the whole problem.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from boundflow import BoundFlowWorker, Complete, WorkflowConfig
from tests.conftest import (
    HAIKU,
    create_isolated_tenant,
    dummy_mock,
    wait_for_lifecycle_state,
)
from boundflow import LifecycleState

WORKFLOW = "interrupted_metrics"
AGENT = "spender"

# Time allowed for an already-written interim report to be persisted server-side.
# Generous on purpose: this only covers the server's own processing, and being wrong
# about it fails the test for a reason that has nothing to do with what it asserts.
REPORT_GRACE_SECONDS = 10


@pytest.mark.asyncio
async def test_a_dead_workers_spend_is_still_recorded(cp, api_key):
    """Spend a little, then hang, then kill the worker mid-operation.

    Nothing completes, so nothing is reported the ordinary way — the only route by
    which this cost can reach the server is an interim report landing on the job row
    and FailRequest promoting it before deleting that row.
    """
    from langchain_anthropic import ChatAnthropic

    worker = BoundFlowWorker(llm=dummy_mock())
    spent = asyncio.Event()

    @worker.workflow(WORKFLOW, version=1)
    async def entry(ctx):
        model = ctx.agent_model(AGENT, ChatAnthropic(model=HAIKU, max_tokens=16,
                                                     api_key=api_key))
        await model.ainvoke("Say hi.")
        # Report explicitly and await it, rather than relying on the automatic
        # post-call report having gone out. Each report is the operation's running
        # total merged into a clone of the committed baseline, so a second one
        # replaces the first — this adds no spend, it only pins when the write left.
        await ctx.report_metrics()
        spent.set()
        # Hang, so the operation is still running when the worker dies. A handler
        # that returned would report its metrics the ordinary way and prove nothing.
        await asyncio.sleep(600)
        return Complete()

    tenant = await create_isolated_tenant(cp, "interrupted")
    wf = await cp.create_workflow(WORKFLOW, tenant.id, config=WorkflowConfig(version=1))
    await cp.activate_workflow(wf.id)

    task = asyncio.create_task(worker.run())
    try:
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=600)
        await asyncio.wait_for(spent.wait(), timeout=120)
        # spent is now set only after an awaited report, so the client has certainly
        # written. Reports are deliberately unacked (#94: a failed write kills the
        # stream rather than blocking the run), so there is no way to observe the
        # server having persisted it — this covers that processing, and only that.
        await asyncio.sleep(REPORT_GRACE_SECONDS)
    finally:
        # Kill the worker where a crash would: after the money is gone, before the
        # operation ends.
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    # The lease expires and the run is swept up as interrupted.
    await wait_for_lifecycle_state(cp, wf.id, LifecycleState.INTERRUPTED, timeout=120)

    # Promotion happens inside FailRequest, immediately after the transition the wait
    # above observed — so it is all but landed already. Polled anyway: reading the
    # instant the state flips is the one ordering this test does not control.
    metrics = None
    for _ in range(20):
        metrics = await cp.get_workflow_metrics(wf.id)
        if metrics.total_cost_usd > 0:
            break
        await asyncio.sleep(0.5)

    assert metrics.total_cost_usd > 0, (
        "an interrupted run reported no spend. The handler awaited report_metrics() "
        "before hanging, so the client wrote it — this is the server having failed to "
        "persist the interim report, or FailRequest deleting the job without "
        f"promoting it. Got {metrics}")
    assert metrics.total_llm_calls >= 1
