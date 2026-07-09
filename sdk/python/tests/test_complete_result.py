"""Complete(result=...) publishes the run's output, persisted on the request and
readable later via get_request_info().result — separate from ctx.context (the
caller's working state threaded op-to-op) since a result is a one-time terminal
value, not something read back mid-run."""
from __future__ import annotations

from boundflow import (
    AwaitApproval,
    BoundFlowWorker,
    Complete,
    LifecycleState,
    Next,
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


async def test_complete_result_persisted_on_request(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("complete_result", version=1)
    async def _entry(ctx):
        return Complete(result={"answer": 42, "status": "done"})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "complete-result")
        wf = await cp.create_workflow("complete_result", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid)
            assert info.result == {"answer": 42, "status": "done"}
        finally:
            await cp.delete_workflow(wf.id)


async def test_complete_without_result_leaves_it_none(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("complete_noresult", version=1)
    async def _entry(ctx):
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "complete-noresult")
        wf = await cp.create_workflow("complete_noresult", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid)
            assert info.result is None
        finally:
            await cp.delete_workflow(wf.id)


async def test_result_is_independent_of_context(cp):
    """A workflow can write to ctx.context throughout the run and publish a
    completely different result at the end — the two aren't the same bag."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("complete_result_chain", version=1)
    async def _entry(ctx):
        ctx.context["scratch"] = "working"
        return Next(operation="step2", context=ctx.context, timeout=30)

    @worker.operation("complete_result_chain", "step2")
    async def _step2(ctx):
        assert ctx.context == {"scratch": "working"}
        return Complete(result={"final": "value"})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "complete-result-chain")
        wf = await cp.create_workflow("complete_result_chain", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid)
            assert info.result == {"final": "value"}
        finally:
            await cp.delete_workflow(wf.id)


async def test_complete_result_from_approve_branch(cp):
    """Complete(result=...) as an approval gate's terminal branch — the branch itself
    is 'null' on the wire (no next op), but the result still has to reach the request."""
    captured: list = []
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("complete_result_approve", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Complete(result={"decision": "approved"}),
            on_reject=Complete(result={"decision": "rejected"}),
            timeout=60,
        )

    @worker.on_approval_requested
    async def _on_approval(request):
        captured.append(request)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "complete-result-approve")
        wf = await cp.create_workflow("complete_result_approve", tenant.id,
                                      config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)

            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL)
            await cp.approve_workflow(wf.id, captured[0].approval_id)
            info = await wait_for_completion(cp, rid)
            assert info.result == {"decision": "approved"}
        finally:
            await cp.delete_workflow(wf.id)


async def test_complete_result_from_reject_branch(cp):
    captured: list = []
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("complete_result_reject", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Complete(result={"decision": "approved"}),
            on_reject=Complete(result={"decision": "rejected"}),
            timeout=60,
        )

    @worker.on_approval_requested
    async def _on_approval(request):
        captured.append(request)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "complete-result-reject")
        wf = await cp.create_workflow("complete_result_reject", tenant.id,
                                      config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)

            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_APPROVAL)
            await cp.reject_workflow(wf.id, captured[0].approval_id)
            info = await wait_for_completion(cp, rid)
            assert info.result == {"decision": "rejected"}
        finally:
            await cp.delete_workflow(wf.id)
