"""Workflow lifecycle policy — self-healing after repeated failures.

Workflow-lifecycle rules evaluate aggregated metrics after each run and can cool
down, roll back, or pause the whole workflow. Here a workflow that keeps failing
pauses itself after 3 failures — a circuit breaker — and further invokes are
rejected until it's resumed.

Deterministic — no Anthropic key needed. Prereqs: backend up + BOUNDFLOW_API_KEY.
Run:  python -m boundflow.examples.self_healing
"""
import asyncio

from boundflow import (
    BoundFlowWorker, Complete, ControlPlaneClient, FailedPreconditionError,
    MockLlmClient, Pause, WorkflowConfig, WorkflowMetric, WorkflowRule,
    WorkflowState, submit,
)


async def main() -> None:
    worker = BoundFlowWorker(llm=MockLlmClient(lambda _: submit()))

    @worker.workflow("flaky", version=1)
    async def _entry(ctx):
        ctx.mark_failed()   # simulate a run that fails
        return Complete()

    task = asyncio.create_task(worker.run())
    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant("self-healing")
        wf = await cp.create_workflow("flaky", tenant.id, config=WorkflowConfig(version=1))
        try:
            # After 3 failures, pause the whole workflow.
            await cp.set_workflow_lifecycle_policy(wf.id, [
                WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=3,
                             action=Pause(window=3)),
            ])
            await cp.activate_workflow(wf.id)

            for i in range(1, 4):
                await _wait_done(cp, await cp.invoke_workflow(wf.id, operation_timeout_seconds=30))
                print(f"  run {i}: failed")

            await _wait_state(cp, wf.id, WorkflowState.PAUSED)
            print("  → 3 failures crossed the threshold; the workflow paused itself.")

            try:
                await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
                print("  (unexpected: invoke was accepted while paused)")
            except FailedPreconditionError:
                print("  → further invokes are rejected while the workflow is paused.")
        finally:
            await cp.delete_workflow(wf.id)
    task.cancel()


async def _wait_done(cp, request_id, timeout=60):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        info = await cp.get_request_info(request_id)
        if info.status.is_terminal():
            return info
        assert asyncio.get_event_loop().time() < deadline, "timed out waiting for the run"
        await asyncio.sleep(0.5)


async def _wait_state(cp, wf_id, expected, timeout=90):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        state = await cp.get_workflow_state(wf_id)
        if state == expected:
            return
        assert asyncio.get_event_loop().time() < deadline, f"timed out; last state={state}"
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
