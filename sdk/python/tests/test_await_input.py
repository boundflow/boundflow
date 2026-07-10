"""AwaitInput — park for a free-form answer (not a binary decision). Mirrors
AwaitApproval's plumbing: WorkflowInfo.pending_input for external discovery (no
dependency on the in-process on_input_requested hook), a durable audit trail via
get_input_audit, and a timeout fallback (on_timeout) when nobody answers."""
from __future__ import annotations

from boundflow import (
    AwaitInput,
    BoundFlowWorker,
    Complete,
    InputDecision,
    InputRequest,
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


async def test_pending_input_discoverable_and_answer_resumes_the_run(cp):
    captured: list[InputRequest] = []
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("await_input_wf", version=1)
    async def _entry(ctx):
        return AwaitInput(
            on_answer=Next(operation="handle_answer", context=ctx.context, timeout=30),
            on_timeout=Complete(result={"decision": "timed_out"}),
            timeout=60,
            prompt="What should I do with this refund?",
            metadata={"ticket": "T-42", "amount_usd": 25},
        )

    @worker.operation("await_input_wf", "handle_answer")
    async def _handle_answer(ctx):
        return Complete(result={"decision": ctx.context["answer"]["choice"]})

    @worker.on_input_requested
    async def _on_input(request: InputRequest):
        captured.append(request)

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "await-input")
        wf = await cp.create_workflow("await_input_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)

            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_INPUT)

            # The in-process hook gets prompt/metadata synchronously, no round-trip.
            assert len(captured) == 1
            assert captured[0].workflow_id == wf.id
            assert captured[0].prompt == "What should I do with this refund?"
            assert captured[0].metadata == {"ticket": "T-42", "amount_usd": 25}
            assert captured[0].input_id

            # Discover the pending input purely from get_workflow too — no in-process
            # hook, no reference to the worker at all beyond this point.
            info = await cp.get_workflow(wf.id)
            pending = info.pending_input
            assert pending is not None
            assert pending.input_id == captured[0].input_id
            assert pending.prompt == "What should I do with this refund?"
            assert pending.metadata == {"ticket": "T-42", "amount_usd": 25}
            assert pending.opened_at is not None
            assert pending.timeout_at is not None

            await cp.submit_input(wf.id, pending.input_id, {"choice": "approve_refund"}, actor="alice")
            result_info = await wait_for_completion(cp, rid)
            assert result_info.result == {"decision": "approve_refund"}

            # Resolved: no longer pending.
            info_after = await cp.get_workflow(wf.id)
            assert info_after.pending_input is None

            # Audited, including the actual submitted answer content.
            audit = await cp.get_input_audit(wf.id)
            assert len(audit) == 1
            assert audit[0].input_id == pending.input_id
            assert audit[0].decision == InputDecision.ANSWERED
            assert audit[0].actor == "alice"
            assert audit[0].answer == {"choice": "approve_refund"}
        finally:
            await cp.delete_workflow(wf.id)


async def test_input_timeout_runs_on_timeout_branch(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("await_input_timeout", version=1)
    async def _entry(ctx):
        return AwaitInput(
            on_answer=Complete(result={"decision": "answered"}),
            on_timeout=Complete(result={"decision": "timed_out"}),
            timeout=8,
        )

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "await-input-timeout")
        wf = await cp.create_workflow("await_input_timeout", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)

            # Wait for it to park, then let the timeout expire (don't submit an answer).
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.AWAITING_INPUT)
            await wait_for_lifecycle_state(cp, wf.id, LifecycleState.ACTIVE, timeout=45)

            result_info = await wait_for_completion(cp, rid)
            assert result_info.result == {"decision": "timed_out"}

            audit = await cp.get_input_audit(wf.id)
            assert len(audit) == 1
            assert audit[0].decision == InputDecision.TIMED_OUT
            assert audit[0].answer is None
        finally:
            await cp.delete_workflow(wf.id)


async def test_pending_input_none_when_not_awaiting(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("await_input_none", version=1)
    async def _entry(ctx):
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "await-input-none")
        wf = await cp.create_workflow("await_input_none", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, rid)

            info = await cp.get_workflow(wf.id)
            assert info.pending_input is None
        finally:
            await cp.delete_workflow(wf.id)
