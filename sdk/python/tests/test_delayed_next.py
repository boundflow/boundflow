"""Next.delay_seconds — hold dispatch of the next operation for N seconds rather
than advancing as soon as it's eligible. End-to-end against a real backend: proves
the migration (dispatch_at column), the dispatch-query predicate, and the wire
plumbing all work together, not just that the Python object carries the field."""
from __future__ import annotations

import time

import pytest

from boundflow import AwaitApproval, AwaitInput, BoundFlowWorker, Complete, Next, WorkflowConfig

from .conftest import WORKER_ADDRESS, create_isolated_tenant, dummy_mock, run_worker, wait_for_completion

DELAY_SECONDS = 20
# rpcworker's idle-poll retry (jobLookupInterval = 5s) means even zero-delay dispatch has real
# jitter — observed up to ~10s worst case locally. Generous margin above that, well under DELAY_SECONDS.
NO_DELAY_UPPER_BOUND = 15


async def test_next_delay_seconds_holds_dispatch(cp):
    started_at: list[float] = []

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("delayed_next_wf", version=1)
    async def _entry(ctx):
        return Next(operation="delayed_step", context=ctx.context, timeout=30,
                    delay_seconds=DELAY_SECONDS)

    @worker.operation("delayed_next_wf", "delayed_step")
    async def _delayed_step(ctx):
        started_at.append(time.monotonic())
        return Complete(result={"done": True})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "delayed-next")
        wf = await cp.create_workflow("delayed_next_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            invoked_at = time.monotonic()
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid, timeout=DELAY_SECONDS + 30)

            assert info.status == "completed"
            assert len(started_at) == 1, "delayed_step should have run exactly once"
            elapsed = started_at[0] - invoked_at
            assert elapsed >= DELAY_SECONDS, \
                f"delayed_step ran after {elapsed:.1f}s, expected to be held at least {DELAY_SECONDS}s"
        finally:
            await cp.delete_workflow(wf.id)


async def test_next_without_delay_seconds_dispatches_promptly(cp):
    """Control case: same shape, no delay — proves DELAY_SECONDS above is actually
    doing something, not just that completion eventually happens either way."""
    started_at: list[float] = []

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("undelayed_next_wf", version=1)
    async def _entry(ctx):
        return Next(operation="step", context=ctx.context, timeout=30)

    @worker.operation("undelayed_next_wf", "step")
    async def _step(ctx):
        started_at.append(time.monotonic())
        return Complete(result={"done": True})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "undelayed-next")
        wf = await cp.create_workflow("undelayed_next_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            invoked_at = time.monotonic()
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, rid, timeout=30)

            assert len(started_at) == 1
            elapsed = started_at[0] - invoked_at
            assert elapsed < NO_DELAY_UPPER_BOUND, \
                f"step took {elapsed:.1f}s with no delay set — expected under {NO_DELAY_UPPER_BOUND}s"
        finally:
            await cp.delete_workflow(wf.id)


async def test_delay_then_no_delay_does_not_leak_across_hops(cp):
    """op1 -> op2 is delayed; op2 -> op3 sets no delay at all. op3 must dispatch
    promptly once op2 finishes — proving dispatch_at is freshly recomputed on
    every Next() transition, not inherited/stale from an earlier hop."""
    op2_started_at: list[float] = []
    op2_finished_at: list[float] = []
    op3_started_at: list[float] = []

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("delay_then_none_wf", version=1)
    async def _entry(ctx):
        return Next(operation="op2", context=ctx.context, timeout=30, delay_seconds=DELAY_SECONDS)

    @worker.operation("delay_then_none_wf", "op2")
    async def _op2(ctx):
        op2_started_at.append(time.monotonic())
        result = Next(operation="op3", context=ctx.context, timeout=30)  # no delay
        op2_finished_at.append(time.monotonic())
        return result

    @worker.operation("delay_then_none_wf", "op3")
    async def _op3(ctx):
        op3_started_at.append(time.monotonic())
        return Complete(result={"done": True})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "delay-then-none")
        wf = await cp.create_workflow("delay_then_none_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            invoked_at = time.monotonic()
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid, timeout=DELAY_SECONDS + 30)

            assert info.status == "completed"
            assert len(op3_started_at) == 1

            op1_to_op2_gap = op2_started_at[0] - invoked_at
            assert op1_to_op2_gap >= DELAY_SECONDS, \
                f"op2 ran after {op1_to_op2_gap:.1f}s, expected to be held at least {DELAY_SECONDS}s"

            op2_to_op3_gap = op3_started_at[0] - op2_finished_at[0]
            assert op2_to_op3_gap < NO_DELAY_UPPER_BOUND, \
                (f"op3 took {op2_to_op3_gap:.1f}s after op2 returned a no-delay Next — "
                 f"expected under {NO_DELAY_UPPER_BOUND}s (stale dispatch_at from op1->op2 leaked through)")
        finally:
            await cp.delete_workflow(wf.id)


async def test_delay_changes_between_hops_not_stale_or_accumulated(cp):
    """op1 -> op2 delayed by DELAY_SECONDS; op2 -> op3 delayed by a different,
    larger value. Each hop's gap must match its own delay_seconds — not the
    other hop's value (stale) and not their sum (accumulated)."""
    # Deliberately far from DELAY_SECONDS (well beyond the ~10s worst-case dispatch
    # jitter established above) so a bug that used the wrong hop's value, or summed
    # both, is unambiguous rather than lost in timing noise.
    second_delay = DELAY_SECONDS + 20
    op2_started_at: list[float] = []
    op2_finished_at: list[float] = []
    op3_started_at: list[float] = []

    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("delay_change_wf", version=1)
    async def _entry(ctx):
        return Next(operation="op2", context=ctx.context, timeout=30, delay_seconds=DELAY_SECONDS)

    @worker.operation("delay_change_wf", "op2")
    async def _op2(ctx):
        op2_started_at.append(time.monotonic())
        result = Next(operation="op3", context=ctx.context, timeout=30, delay_seconds=second_delay)
        op2_finished_at.append(time.monotonic())
        return result

    @worker.operation("delay_change_wf", "op3")
    async def _op3(ctx):
        op3_started_at.append(time.monotonic())
        return Complete(result={"done": True})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "delay-change")
        wf = await cp.create_workflow("delay_change_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            invoked_at = time.monotonic()
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid, timeout=DELAY_SECONDS + second_delay + 30)

            assert info.status == "completed"
            assert len(op3_started_at) == 1

            op1_to_op2_gap = op2_started_at[0] - invoked_at
            assert op1_to_op2_gap >= DELAY_SECONDS, \
                f"op2 ran after {op1_to_op2_gap:.1f}s, expected at least {DELAY_SECONDS}s"
            assert op1_to_op2_gap < DELAY_SECONDS + NO_DELAY_UPPER_BOUND, \
                f"op2's gap ({op1_to_op2_gap:.1f}s) looks like it used second_delay, not its own"

            op2_to_op3_gap = op3_started_at[0] - op2_finished_at[0]
            assert op2_to_op3_gap >= second_delay, \
                f"op3 ran after {op2_to_op3_gap:.1f}s, expected at least {second_delay}s"
            assert op2_to_op3_gap < second_delay + NO_DELAY_UPPER_BOUND, \
                (f"op3's gap ({op2_to_op3_gap:.1f}s) looks like it accumulated both hops' "
                 f"delays instead of only using its own")
        finally:
            await cp.delete_workflow(wf.id)


def test_delay_seconds_rejected_on_approval_branch():
    delayed = Next(operation="x", context={}, timeout=30, delay_seconds=5)
    with pytest.raises(ValueError, match="delay_seconds"):
        AwaitApproval(on_approve=delayed, on_reject=Complete(), timeout=60)
    with pytest.raises(ValueError, match="delay_seconds"):
        AwaitApproval(on_approve=Complete(), on_reject=delayed, timeout=60)


def test_delay_seconds_rejected_on_input_branch():
    delayed = Next(operation="x", context={}, timeout=30, delay_seconds=5)
    with pytest.raises(ValueError, match="delay_seconds"):
        AwaitInput(on_answer=delayed, on_timeout=Complete(), timeout=60)
    with pytest.raises(ValueError, match="delay_seconds"):
        AwaitInput(on_answer=Complete(), on_timeout=delayed, timeout=60)


def test_delay_seconds_zero_allowed_on_branches():
    """The default (no delay) must still work fine on branches — only a nonzero
    delay_seconds is rejected."""
    plain = Next(operation="x", context={}, timeout=30)
    AwaitApproval(on_approve=plain, on_reject=Complete(), timeout=60)
    AwaitInput(on_answer=plain, on_timeout=Complete(), timeout=60)
