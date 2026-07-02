"""End-to-end tests for ListWorkflowRuns / run_outcome."""
from __future__ import annotations

import asyncio

from boundflow import BoundFlowWorker, Complete, WorkflowConfig

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
)


async def test_list_workflow_runs_reports_each_outcome(cp):
    """A workflow's runs list reports the customer-facing outcome per run: a clean run
    is successful; mark_failed(), a raised exception, and a timeout are all reported as
    (soft) failures with the right outcome, and the exception carries its reason."""
    mode = {"v": "success"}
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("runs_test", version=1)
    async def _entry(ctx):
        m = mode["v"]
        if m == "marked":
            ctx.mark_failed()
            return Complete()
        if m == "exception":
            raise RuntimeError("boom detail")
        if m == "timeout":
            await asyncio.sleep(60)  # exceed the op timeout
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "runs")
        wf = await cp.create_workflow("runs_test", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)

        expected: dict[str, str] = {}

        async def invoke(kind: str, outcome: str, timeout: int = 30):
            mode["v"] = kind
            request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=timeout)
            await wait_for_completion(cp, wf.id)  # runs are sequential
            expected[request_id] = outcome

        await invoke("success", "successful")
        await invoke("marked", "customer_marked_failure")
        await invoke("exception", "uncaught_operation_exception")
        await invoke("timeout", "operation_timeout", timeout=3)

        runs = await cp.list_workflow_runs(wf.id)
        got = {r.request_id: r.run_outcome for r in runs}
        for request_id, outcome in expected.items():
            assert got.get(request_id) == outcome, f"{request_id}: got {got.get(request_id)}, want {outcome}"

        # The uncaught exception surfaces its message as the failure reason.
        exc_run = next(r for r in runs if r.run_outcome == "uncaught_operation_exception")
        assert "boom detail" in exc_run.failure_reason
