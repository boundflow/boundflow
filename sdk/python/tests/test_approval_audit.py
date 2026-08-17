"""Approval audit log + the approval_id correlation between trace and audit.

Telemetry carries only the approval_id (on the await_approval OperationTrace); the
decision / actor / timing live server-side in the audit log, fetched by approval_id
via GetApprovalAudit. Timeouts are resolved (and audited) by the scheduler.
"""
from __future__ import annotations

import asyncio

from boundflow import (
    ApprovalRequest,
    AwaitApproval,
    BoundFlowWorker,
    Complete,
    LifecycleState,
    Next,
    WorkflowConfig,
)
from boundflow.trace import OperationTrace

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
    wait_for_lifecycle_state,
)


class CapturingSink:
    def __init__(self) -> None:
        self.traces: list[OperationTrace] = []

    async def emit(self, trace: OperationTrace) -> None:
        self.traces.append(trace)


async def _wait_for_audit(cp, approval_id: str, timeout: int = 15):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        rec = await cp.get_approval_audit_by_id(approval_id)
        if rec is not None:
            return rec
        assert asyncio.get_event_loop().time() < deadline, f"audit row for {approval_id} never appeared"
        await asyncio.sleep(0.3)


async def test_approve_audits_and_correlates_with_trace(cp):
    captured: list[ApprovalRequest] = []
    sink = CapturingSink()
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock(), trace_sink=sink)

    @worker.workflow("audit_approve", version=1)
    async def _entry(ctx):
        return AwaitApproval(on_approve=Complete(), on_reject=Complete(), timeout=60,
                             justification="sign-off")

    @worker.on_approval_requested
    async def _on(req: ApprovalRequest):
        captured.append(req)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "audit-approve")
        wf = await cp.create_workflow("audit_approve", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL)

            assert len(captured) == 1
            approval_id = captured[0].approval_id

            # The trace carries the SAME approval_id (minted once, shared) and the
            # await_approval outcome — but NOT the decision. That's the whole design.
            t = next(t for t in sink.traces if t.outcome == "await_approval")
            assert t.approval_id == approval_id
            assert t.workflow_id == wf.id

            await cp.approve_workflow(wf.id, approval_id, actor="alice@corp.com")
            await wait_for_completion(cp, request_id)

            # The decision / actor / timing live server-side, looked up by approval_id.
            r = await _wait_for_audit(cp, approval_id)
            assert r.approval_id == approval_id
            assert r.workflow_id == wf.id
            assert r.decision == "approved"
            assert r.actor == "alice@corp.com"
            assert r.opened_at is not None and r.decided_at is not None
            assert r.decided_at >= r.opened_at
        finally:
            await cp.delete_workflow(wf.id)


async def test_reject_audits_with_actor(cp):
    captured: list[ApprovalRequest] = []
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("audit_reject", version=1)
    async def _entry(ctx):
        return AwaitApproval(on_approve=Complete(), on_reject=Complete(), timeout=60)

    @worker.on_approval_requested
    async def _on(req: ApprovalRequest):
        captured.append(req)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "audit-reject")
        wf = await cp.create_workflow("audit_reject", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL)
            approval_id = captured[0].approval_id

            await cp.reject_workflow(wf.id, approval_id, actor="bob@corp.com")
            await wait_for_completion(cp, request_id)

            r = await _wait_for_audit(cp, approval_id)
            assert r.decision == "rejected"
            assert r.actor == "bob@corp.com"
        finally:
            await cp.delete_workflow(wf.id)


async def test_timeout_audits_as_timed_out(cp):
    captured: list[ApprovalRequest] = []
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("audit_timeout", version=1)
    async def _entry(ctx):
        return AwaitApproval(on_approve=Complete(), on_reject=Complete(), timeout=5)

    @worker.on_approval_requested
    async def _on(req: ApprovalRequest):
        captured.append(req)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "audit-timeout")
        wf = await cp.create_workflow("audit_timeout", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL)
            approval_id = captured[0].approval_id

            # No decision: let the gate expire. The scheduler resolver (≤30s tick)
            # rejects it and writes a timed_out audit row with no actor.
            r = await _wait_for_audit(cp, approval_id, timeout=60)
            assert r.decision == "timed_out"
            assert r.actor == ""
            assert r.opened_at is not None
        finally:
            await cp.delete_workflow(wf.id)


async def test_decision_records_justification_and_reason(cp):
    """get_audit_log alone should answer what was proposed, who rejected it, and why —
    with no reconstruction from the run's context."""
    captured: list[ApprovalRequest] = []
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("audit_reason", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Complete(), on_reject=Complete(), timeout=60,
            justification="Refund $250 to customer T-42",
        )

    @worker.on_approval_requested
    async def _on(req: ApprovalRequest):
        captured.append(req)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "audit-reason")
        wf = await cp.create_workflow("audit_reason", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL, timeout=30)

            approval_id = captured[0].approval_id
            await cp.reject_workflow(wf.id, approval_id, actor="arjun@example.com",
                                     reason="Refund exceeds the $100 auto-approve threshold")
            await wait_for_completion(cp, rid, timeout=60)

            rec = await _wait_for_audit(cp, approval_id)
            # What was asked for — copied off the job row before the run cleared it.
            assert rec.justification == "Refund $250 to customer T-42"
            # Who, and why — captured in the same call as the decision.
            assert rec.actor == "arjun@example.com"
            assert rec.reason == "Refund exceeds the $100 auto-approve threshold"
            assert rec.decision.value == "rejected"

            # The same content has to be on the unified log, since that's the corpus.
            entries = await cp.get_audit_log(wf.id)
            approvals = [e for e in entries if getattr(e, "approval_id", None) == approval_id]
            assert approvals, "approval decision missing from the unified audit log"
            assert approvals[0].justification == "Refund $250 to customer T-42"
            assert approvals[0].reason == "Refund exceeds the $100 auto-approve threshold"
        finally:
            await cp.delete_workflow(wf.id)


async def test_reason_is_optional_and_approve_carries_it_too(cp):
    captured: list[ApprovalRequest] = []
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("audit_reason_approve", version=1)
    async def _entry(ctx):
        return AwaitApproval(on_approve=Complete(), on_reject=Complete(), timeout=60,
                             justification="Deploy v2 to prod")

    @worker.on_approval_requested
    async def _on(req: ApprovalRequest):
        captured.append(req)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "audit-reason-approve")
        wf = await cp.create_workflow("audit_reason_approve", tenant.id,
                                      config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL, timeout=30)

            approval_id = captured[0].approval_id
            await cp.approve_workflow(wf.id, approval_id, actor="ops",
                                      reason="Canary looked clean for 24h")
            await wait_for_completion(cp, rid, timeout=60)

            rec = await _wait_for_audit(cp, approval_id)
            assert rec.decision.value == "approved"
            assert rec.reason == "Canary looked clean for 24h"
            assert rec.justification == "Deploy v2 to prod"
        finally:
            await cp.delete_workflow(wf.id)


async def test_timeout_still_records_what_went_unanswered(cp):
    """A timeout has no decider and so no reason, but the audit row should still say
    what was being asked."""
    captured: list[ApprovalRequest] = []
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("audit_timeout_just", version=1)
    async def _entry(ctx):
        return AwaitApproval(on_approve=Complete(), on_reject=Complete(), timeout=8,
                             justification="Waive the late fee for account 991")

    @worker.on_approval_requested
    async def _on(req: ApprovalRequest):
        captured.append(req)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "audit-timeout-just")
        wf = await cp.create_workflow("audit_timeout_just", tenant.id,
                                      config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL, timeout=30)

            # Let it expire — nobody decides.
            rec = await _wait_for_audit(cp, captured[0].approval_id, timeout=60)
            assert rec.decision.value == "timed_out"
            assert rec.justification == "Waive the late fee for account 991"
            assert rec.reason == "", "a timeout has no decider, so no reason"
            assert rec.actor == ""
        finally:
            await cp.delete_workflow(wf.id)


async def test_reason_reaches_the_resumed_branch(cp):
    """The point of collapsing AwaitInput("why?") into the rejection: the on_reject
    operation can still *act* on the reason, not just find it in the audit log."""
    captured: list[ApprovalRequest] = []
    seen: dict = {}
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("reason_to_branch", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Complete(),
            on_reject=Next(operation="handle_rejection", context=ctx.context, timeout=30),
            timeout=60,
            justification="Ship v3 on Friday",
        )

    @worker.operation("reason_to_branch", "handle_rejection")
    async def _handle(ctx):
        seen["reason"] = ctx.approval_reason
        # Popped, so it doesn't leak into whatever this operation passes onward.
        seen["after_pop"] = ctx.approval_reason
        return Complete(result={"noted": seen["reason"]})

    @worker.on_approval_requested
    async def _on(req: ApprovalRequest):
        captured.append(req)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "reason-branch")
        wf = await cp.create_workflow("reason_to_branch", tenant.id,
                                      config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL, timeout=30)

            await cp.reject_workflow(wf.id, captured[0].approval_id, actor="lead",
                                     reason="Freeze window starts Thursday")
            info = await wait_for_completion(cp, rid, timeout=60)

            assert seen["reason"] == "Freeze window starts Thursday", \
                f"on_reject branch didn't receive the reason, got {seen.get('reason')!r}"
            assert seen["after_pop"] is None, "approval_reason should be popped after reading"
            assert info.result == {"noted": "Freeze window starts Thursday"}
        finally:
            await cp.delete_workflow(wf.id)


async def test_no_reason_means_branch_sees_none(cp):
    captured: list[ApprovalRequest] = []
    seen: dict = {}
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("no_reason_branch", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="after", context=ctx.context, timeout=30),
            on_reject=Complete(), timeout=60)

    @worker.operation("no_reason_branch", "after")
    async def _after(ctx):
        seen["reason"] = ctx.approval_reason
        return Complete()

    @worker.on_approval_requested
    async def _on(req: ApprovalRequest):
        captured.append(req)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "no-reason-branch")
        wf = await cp.create_workflow("no_reason_branch", tenant.id,
                                      config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL, timeout=30)
            await cp.approve_workflow(wf.id, captured[0].approval_id, actor="lead")
            await wait_for_completion(cp, rid, timeout=60)
            assert seen["reason"] is None
        finally:
            await cp.delete_workflow(wf.id)
