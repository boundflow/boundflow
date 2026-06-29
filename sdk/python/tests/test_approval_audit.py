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
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL)

            assert len(captured) == 1
            approval_id = captured[0].approval_id

            # The trace carries the SAME approval_id (minted once, shared) and the
            # await_approval outcome — but NOT the decision. That's the whole design.
            t = next(t for t in sink.traces if t.outcome == "await_approval")
            assert t.approval_id == approval_id
            assert t.workflow_id == wf.id

            await cp.approve_workflow(wf.id, approval_id, actor="alice@corp.com")
            await wait_for_completion(cp, wf.id)

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
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL)
            approval_id = captured[0].approval_id

            await cp.reject_workflow(wf.id, approval_id, actor="bob@corp.com")
            await wait_for_completion(cp, wf.id)

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
