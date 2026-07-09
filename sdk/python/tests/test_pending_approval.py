"""WorkflowInfo.pending_approval — while a workflow is AWAITING_APPROVAL, an external
caller can discover the approval_id (+ justification, metadata) via get_workflow,
with no dependency on the in-process worker's on_approval_requested hook. Proves the
"page reload" case: approve using only what get_workflow returned."""
from __future__ import annotations

from boundflow import (
    AwaitApproval,
    BoundFlowWorker,
    Complete,
    LifecycleState,
    WorkflowConfig,
)

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
    wait_for_lifecycle_state,
)


async def test_pending_approval_discoverable_via_get_workflow(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("pending_approval_wf", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Complete(result={"decision": "approved"}),
            on_reject=Complete(result={"decision": "rejected"}),
            timeout=60,
            justification="needs human sign-off",
            metadata={"ticket": "T-42", "risk": "low"},
        )

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "pending-approval")
        wf = await cp.create_workflow("pending_approval_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)

            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL)

            # Discover the pending approval purely from get_workflow — no in-process
            # hook, no reference to the worker at all beyond this point.
            info = await cp.get_workflow(wf.id)
            pending = info.pending_approval
            assert pending is not None
            assert pending.approval_id
            assert pending.justification == "needs human sign-off"
            assert pending.metadata == {"ticket": "T-42", "risk": "low"}
            assert pending.opened_at is not None
            assert pending.timeout_at is not None

            await cp.approve_workflow(wf.id, pending.approval_id)
            result_info = await wait_for_completion(cp, rid)
            assert result_info.result == {"decision": "approved"}

            # Resolved: no longer pending.
            info_after = await cp.get_workflow(wf.id)
            assert info_after.pending_approval is None
        finally:
            await cp.delete_workflow(wf.id)


async def test_pending_approval_none_when_not_awaiting(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("pending_approval_none", version=1)
    async def _entry(ctx):
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "pending-approval-none")
        wf = await cp.create_workflow("pending_approval_none", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, rid)

            info = await cp.get_workflow(wf.id)
            assert info.pending_approval is None
        finally:
            await cp.delete_workflow(wf.id)
