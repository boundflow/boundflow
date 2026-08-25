"""End-to-end tests for SuspendWorkflow / ResumeWorkflow."""
from __future__ import annotations

import asyncio

import pytest

from boundflow import (
    BoundFlowWorker,
    Complete,
    FailedPreconditionError,
    InvokeMode,
    LifecycleState,
    RunOutcome,
    RunStatus,
    WorkflowConfig,
    WorkflowState,
)

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
    wait_for_lifecycle_state,
    wait_for_workflow_state,
)

REPEAT_EVERY = 30  # the scheduler polls periodic workflows every 30s; that's the floor


async def _suspended_workflow(cp, prefix: str, handler):
    """A workflow with one registered handler, activated and ready to invoke."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    worker.workflow(f"{prefix}_wf", version=1)(handler)
    tenant = await create_isolated_tenant(cp, prefix)
    workflow = await cp.create_workflow(f"{prefix}_wf", tenant.id, config=WorkflowConfig(version=1))
    await cp.activate_workflow(workflow.id)
    return worker, workflow


async def test_suspend_holds_queued_work_and_resume_releases_it(cp):
    """Suspending an idle workflow holds it immediately: invokes are refused while held,
    and the queued request is released and runs once resumed."""
    async def _entry(ctx):
        return Complete()

    worker, workflow = await _suspended_workflow(cp, "susp", _entry)

    async with run_worker(worker):
        first = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        await wait_for_completion(cp, first, timeout=60)

        suspension_id = await cp.suspend_workflow(workflow.id, reason="operator hold")
        assert suspension_id

        await wait_for_workflow_state(cp, workflow.id, WorkflowState.SUSPENDED)
        info = await cp.get_workflow(workflow.id)
        assert info.suspension is not None
        assert info.suspension.suspension_id == suspension_id
        assert info.suspension.reason == "operator hold"
        assert info.suspension.requested_at is not None

        # Nothing was running, so it drains immediately.
        for _ in range(60):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized"

        # A held workflow refuses new work outright.
        with pytest.raises(FailedPreconditionError):
            await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)

        # Wrong id can't release it.
        with pytest.raises(FailedPreconditionError):
            await cp.resume_workflow(workflow.id, "not-the-right-id")

        await cp.resume_workflow(workflow.id, suspension_id)
        info = await cp.get_workflow(workflow.id)
        assert info.workflow_state == WorkflowState.ACTIVE
        assert info.suspension is None

        # ...and it runs again.
        second = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        run = await wait_for_completion(cp, second, timeout=60)
        assert run.status == RunStatus.COMPLETED


async def test_suspend_stop_current_cuts_the_running_run(cp):
    """stop_current_run cuts the in-flight run rather than draining it. The run is recorded
    as SUSPENDED — deliberately stopped, so it must not count as a workflow failure."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def _entry(ctx):
        started.set()
        if not release.is_set():
            await asyncio.sleep(120)
        return Complete()

    worker, workflow = await _suspended_workflow(cp, "cut", _entry)

    async with run_worker(worker):
        request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        await asyncio.wait_for(started.wait(), timeout=30)
        await asyncio.sleep(2)  # let the server settle into its busy state

        suspension_id = await cp.suspend_workflow(
            workflow.id, reason="cut it", stop_current_run=True)

        run = await wait_for_completion(cp, request_id, timeout=90)
        assert run.run_outcome == RunOutcome.SUSPENDED, f"got {run.run_outcome}"

        for _ in range(120):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized"

        await cp.resume_workflow(workflow.id, suspension_id)
        assert (await cp.get_workflow(workflow.id)).workflow_state == WorkflowState.ACTIVE

        release.set()  # let the next run finish promptly
        rerun = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        assert (await wait_for_completion(cp, rerun, timeout=60)).status == RunStatus.COMPLETED


async def test_suspend_drains_the_running_run_by_default(cp):
    """Without stop_current_run the in-flight run is left to finish: it completes normally
    (not SUSPENDED), and only then does the suspension finalize."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def _entry(ctx):
        started.set()
        await release.wait()
        return Complete()

    worker, workflow = await _suspended_workflow(cp, "drain", _entry)

    async with run_worker(worker):
        request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        await asyncio.wait_for(started.wait(), timeout=30)
        await asyncio.sleep(2)

        suspension_id = await cp.suspend_workflow(workflow.id, reason="drain it")
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.SUSPENDED)

        # Still draining: the run is untouched, so the suspension can't have finalized.
        info = await cp.get_workflow(workflow.id)
        assert info.suspension.finalized_at is None, "finalized while a run was still going"

        release.set()
        run = await wait_for_completion(cp, request_id, timeout=90)
        assert run.run_outcome == RunOutcome.SUCCESSFUL, f"drained run should succeed, got {run.run_outcome}"

        for _ in range(120):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized after the run drained"
        assert info.lifecycle_state == LifecycleState.HALTED

        await cp.resume_workflow(workflow.id, suspension_id)
        assert (await cp.get_workflow(workflow.id)).workflow_state == WorkflowState.ACTIVE

        # A run completed while suspended, so its lifecycle policy resolution was dropped
        # (TryApplyStateResolution refuses to write to a suspended workflow) and
        # lifecycle_last_resolved fell behind current_version. Unless the resume catches it
        # up, validateWorkflowState silently refuses to schedule anything ever again.
        rerun = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        assert (await wait_for_completion(cp, rerun, timeout=60)).status == RunStatus.COMPLETED


async def test_periodic_workflow_stops_firing_while_suspended_and_resumes_on_its_own(cp):
    """A suspension stops periodic firing without anyone cancelling the schedule, and
    resuming brings it back with no explicit invoke — the workflow picks itself back up."""
    firings: list[float] = []

    async def _entry(ctx):
        firings.append(asyncio.get_event_loop().time())
        return Complete()

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    worker.workflow("periodic_susp_wf", version=1)(_entry)
    tenant = await create_isolated_tenant(cp, "periodic-susp")
    workflow = await cp.create_workflow(
        "periodic_susp_wf", tenant.id,
        config=WorkflowConfig(version=1, invoke_timeout_seconds=30,
                              repeat_every_seconds=REPEAT_EVERY))

    async with run_worker(worker):
        await cp.activate_workflow(workflow.id)

        # One automatic firing to prove the schedule is live.
        deadline = asyncio.get_event_loop().time() + 180
        while not firings:
            assert asyncio.get_event_loop().time() < deadline, "periodic never fired"
            await asyncio.sleep(0.5)

        suspension_id = await cp.suspend_workflow(workflow.id, reason="pause the schedule")
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.SUSPENDED)
        for _ in range(120):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized"

        # Nothing fires while held, across more than a full period.
        held = len(firings)
        await asyncio.sleep(REPEAT_EVERY * 1.5)
        assert len(firings) == held, f"periodic fired {len(firings) - held}x while suspended"

        # Resuming restores the schedule with no invoke of our own.
        await cp.resume_workflow(workflow.id, suspension_id)
        deadline = asyncio.get_event_loop().time() + 180
        while len(firings) == held:
            assert asyncio.get_event_loop().time() < deadline, "periodic never resumed firing"
            await asyncio.sleep(0.5)

    await cp.delete_workflow(workflow.id)


async def test_interruption_during_drain_supersedes_the_suspension(cp):
    """A platform failure on the run a suspension is draining outranks the suspension: the
    run may have left external state half-written, so the workflow interrupts and waits for
    a human rather than quietly finishing the hold. The suspension is torn down in the same
    write, so resolving the interruption is a complete recovery — no residue left to block
    future suspends, and no requests stranded in the held state."""
    started = asyncio.Event()

    async def _entry(ctx):
        started.set()
        await asyncio.sleep(120)
        return Complete()

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    worker.workflow("interrupt_susp_wf", version=1)(_entry)
    tenant = await create_isolated_tenant(cp, "interrupt-susp")
    workflow = await cp.create_workflow(
        "interrupt_susp_wf", tenant.id, config=WorkflowConfig(version=1))
    await cp.activate_workflow(workflow.id)

    # Run the worker by hand so it can be killed mid-operation (a real stream drop).
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)
    try:
        request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        await asyncio.wait_for(started.wait(), timeout=30)
        await asyncio.sleep(2)

        await cp.suspend_workflow(workflow.id, reason="drain then break")
        await wait_for_workflow_state(cp, workflow.id, WorkflowState.SUSPENDED)
        assert (await cp.get_workflow(workflow.id)).suspension.finalized_at is None
    finally:
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

    # The lost worker interrupts the workflow, and the suspension goes with it.
    await wait_for_lifecycle_state(cp, workflow.id, LifecycleState.INTERRUPTED, timeout=120)
    info = await cp.get_workflow(workflow.id)
    assert info.workflow_state == WorkflowState.DISABLED
    assert info.suspension is None, "suspension survived an interruption"
    assert info.last_interrupted_request_id == request_id

    # Resume is meaningless now — there is no suspension to release.
    with pytest.raises(FailedPreconditionError):
        await cp.resume_workflow(workflow.id, "any-id")

    # Acknowledging the interruption is the whole recovery.
    await cp.resolve_interrupted_workflow(workflow.id, request_id)
    info = await cp.get_workflow(workflow.id)
    assert info.workflow_state == WorkflowState.ACTIVE
    assert info.lifecycle_state == LifecycleState.ACTIVE

    recovery_worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @recovery_worker.workflow("interrupt_susp_wf", version=1)
    async def _recovered(ctx):
        return Complete()

    async with run_worker(recovery_worker):
        rerun = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        assert (await wait_for_completion(cp, rerun, timeout=60)).status == RunStatus.COMPLETED

    # And a fresh suspension still works, proving no residue was left behind.
    suspension_id = await cp.suspend_workflow(workflow.id, reason="after recovery")
    assert suspension_id
    await wait_for_workflow_state(cp, workflow.id, WorkflowState.SUSPENDED)


async def _queued_behind_a_running_run(cp, prefix: str, release: asyncio.Event, started: asyncio.Event):
    """A queue-mode workflow with one run in flight and a second request queued behind it."""
    async def _entry(ctx):
        started.set()
        await release.wait()
        return Complete()

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    worker.workflow(f"{prefix}_wf", version=1)(_entry)
    tenant = await create_isolated_tenant(cp, prefix)
    workflow = await cp.create_workflow(
        f"{prefix}_wf", tenant.id,
        config=WorkflowConfig(version=1, invoke_timeout_seconds=120,
                              invoke_mode=InvokeMode.QUEUE))
    await cp.activate_workflow(workflow.id)
    return worker, workflow


async def test_suspend_holds_queued_requests_for_the_resume(cp):
    """By default queued work is held, not discarded: it keeps its place, sits out the
    suspension, and runs once resumed."""
    started, release = asyncio.Event(), asyncio.Event()
    worker, workflow = await _queued_behind_a_running_run(cp, "hold", release, started)

    async with run_worker(worker):
        first = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        await asyncio.wait_for(started.wait(), timeout=30)
        # Queue mode: one job per workflow, so this waits rather than superseding.
        second = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        assert (await cp.get_request_info(second)).status == RunStatus.UNSCHEDULED

        suspension_id = await cp.suspend_workflow(workflow.id, reason="hold the queue")
        assert (await cp.get_request_info(second)).status == RunStatus.PAUSED

        release.set()
        await wait_for_completion(cp, first, timeout=90)
        for _ in range(120):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized"

        # Held across the whole suspension — it must not sneak through.
        assert (await cp.get_request_info(second)).status == RunStatus.PAUSED

        await cp.resume_workflow(workflow.id, suspension_id)
        assert (await wait_for_completion(cp, second, timeout=90)).status == RunStatus.COMPLETED


async def test_suspend_with_abandon_queued_discards_held_requests(cp):
    """abandon_queued=True fails the backlog instead of holding it — the irreversible
    option, so whoever invoked isn't left waiting for the length of the suspension. Those
    requests stay abandoned after the resume."""
    started, release = asyncio.Event(), asyncio.Event()
    worker, workflow = await _queued_behind_a_running_run(cp, "discard", release, started)

    async with run_worker(worker):
        first = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        await asyncio.wait_for(started.wait(), timeout=30)
        second = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=120)
        assert (await cp.get_request_info(second)).status == RunStatus.UNSCHEDULED

        suspension_id = await cp.suspend_workflow(
            workflow.id, reason="drop the queue", abandon_queued=True)
        assert (await cp.get_request_info(second)).status == RunStatus.ABANDONED

        release.set()
        await wait_for_completion(cp, first, timeout=90)
        for _ in range(120):
            info = await cp.get_workflow(workflow.id)
            if info.suspension and info.suspension.finalized_at is not None:
                break
            await asyncio.sleep(0.5)
        assert info.suspension.finalized_at is not None, "suspension never finalized"

        await cp.resume_workflow(workflow.id, suspension_id)
        # Abandoned is terminal: the resume must not resurrect it.
        assert (await cp.get_request_info(second)).status == RunStatus.ABANDONED

        # The workflow itself is fine and takes new work.
        third = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
        assert (await wait_for_completion(cp, third, timeout=90)).status == RunStatus.COMPLETED
