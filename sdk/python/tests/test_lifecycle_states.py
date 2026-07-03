"""End-to-end tests for the run-phase lifecycle states: scheduled, blocked, invoking.

These states are a projection of the workflow's in-flight job. The scheduler reconciles
them on a ~30s tick, and a run is reported `blocked` once it has sat unowned for longer
than the blocked threshold (60s). To stay off race conditions we only assert *settled*
states: with no worker connected a run deterministically parks in scheduled -> blocked and
stays there, and a slow handler pins `invoking` for the whole poll window. We never try to
catch a fleeting mid-run transition.
"""
from __future__ import annotations

import asyncio

from boundflow import BoundFlowWorker, Complete, LifecycleState, WorkflowConfig

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
    wait_for_lifecycle_state,
)

# The scheduler ticks every ~30s and reports a run blocked after it has been unowned for
# 60s, so blocked can take up to ~90s to surface. Give it comfortable headroom.
BLOCKED_TIMEOUT = 150


async def test_unstarted_run_is_scheduled_then_blocked(cp):
    """With no worker to pick it up, an invoked run parks in `scheduled` and, once it has
    waited past the blocked threshold, is reported `blocked` — and stays there. Both are
    stable (no worker => no dispatch), so polling for them can't race a transition."""
    tenant = await create_isolated_tenant(cp, "lifecycle-blocked")
    workflow = await cp.create_workflow(
        "lifecycle_blocked", tenant.id, config=WorkflowConfig(version=1)
    )
    try:
        await cp.activate_workflow(workflow.id)

        # No worker is ever started, so nothing dispatches this run.
        await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)

        # It first shows up as scheduled (waiting for a worker)...
        await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.SCHEDULED, timeout=60)
        # ...then, having sat unowned past the threshold, as blocked.
        await wait_for_lifecycle_state(
            cp, workflow.id, LifecycleState.BLOCKED, timeout=BLOCKED_TIMEOUT
        )
    finally:
        await cp.delete_workflow(workflow.id)


async def test_blocked_run_recovers_when_a_worker_appears(cp):
    """A blocked run isn't terminal: once a worker connects it gets dispatched, runs, and
    the workflow settles back to `active`."""
    tenant = await create_isolated_tenant(cp, "lifecycle-recover")
    workflow = await cp.create_workflow(
        "lifecycle_recover", tenant.id, config=WorkflowConfig(version=1)
    )

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("lifecycle_recover", version=1)
    async def _entry(ctx):
        return Complete()

    try:
        await cp.activate_workflow(workflow.id)
        request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)

        # Let it get reported blocked before any worker exists.
        await wait_for_lifecycle_state(
            cp, workflow.id, LifecycleState.BLOCKED, timeout=BLOCKED_TIMEOUT
        )

        # Now a worker shows up and drains the run.
        async with run_worker(worker):
            info = await wait_for_completion(cp, request_id)
        assert info.run_outcome == "successful", f"expected successful, got {info.run_outcome}"

        # After the run the workflow is back to active (no longer blocked/invoking).
        await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.ACTIVE, timeout=60)
    finally:
        await cp.delete_workflow(workflow.id)


async def test_in_flight_run_is_invoking_then_returns_to_active(cp):
    """While a worker is actively executing the operation the workflow reports `invoking`,
    and once the run completes it settles back to `active`. The handler blocks on an event
    the test controls, so `invoking` is held steady for the whole assertion — not raced."""
    release = asyncio.Event()
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("lifecycle_invoking", version=1)
    async def _entry(ctx):
        await asyncio.wait_for(release.wait(), timeout=30)
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "lifecycle-invoking")
        workflow = await cp.create_workflow(
            "lifecycle_invoking", tenant.id, config=WorkflowConfig(version=1)
        )
        try:
            await cp.activate_workflow(workflow.id)
            request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)

            # The op is parked inside the handler, so invoking is pinned while we assert it.
            await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.INVOKING, timeout=30)

            # Release the handler and let the run finish.
            release.set()
            info = await wait_for_completion(cp, request_id)
            assert info.run_outcome == "successful", f"expected successful, got {info.run_outcome}"

            await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.ACTIVE, timeout=60)
        finally:
            release.set()
            await cp.delete_workflow(workflow.id)
