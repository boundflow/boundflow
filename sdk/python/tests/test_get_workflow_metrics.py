"""GetWorkflowMetrics — cumulative totals for a workflow's current version.

Not windowed, never trimmed: a fresh workflow reports zero-value totals, and a
completed run rolls up into run_count/latency/etc.
"""
from __future__ import annotations

from boundflow import BoundFlowWorker, Complete, WorkflowConfig, WorkflowMetrics

from .conftest import (
    WORKER_ADDRESS, create_isolated_tenant, dummy_mock, run_worker, wait_for_completion,
)


async def test_get_workflow_metrics_zero_value_before_any_run(cp):
    tenant = await create_isolated_tenant(cp, "metrics-zero")
    wf = await cp.create_workflow("metrics-zero-wf", tenant.id, config=WorkflowConfig(version=1))

    metrics = await cp.get_workflow_metrics(wf.id)

    assert isinstance(metrics, WorkflowMetrics)
    assert metrics.version == 1
    assert metrics.run_count == 0
    assert metrics.total_cost_usd == 0
    assert metrics.total_failures == 0
    assert metrics.total_llm_calls == 0
    assert metrics.total_latency_seconds == 0
    assert metrics.total_approval_rejections == 0
    assert metrics.tool_failure_counts == {}


async def test_get_workflow_metrics_reflects_completed_run(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("metrics-run-wf", version=1)
    async def _entry(ctx):
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "metrics-run")
        wf = await cp.create_workflow("metrics-run-wf", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)

        request_id = await cp.invoke_workflow(wf.id)
        await wait_for_completion(cp, request_id)

        metrics = await cp.get_workflow_metrics(wf.id)
        assert metrics.run_count == 1
        assert metrics.version == 1
