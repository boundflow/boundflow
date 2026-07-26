"""Metrics and policy decisions only ever consider a version's latest epoch.

Redeploying a version (v1 -> v2 -> v1) starts a fresh metrics epoch for v1, so an
older epoch's already-accumulated failures must not leak into either
get_workflow_metrics or workflow-lifecycle rule evaluation for the new epoch.
"""
from __future__ import annotations

from boundflow import (
    BoundFlowWorker, Complete, SetVersion, WorkflowConfig, WorkflowMetric, WorkflowRule,
)

from .conftest import create_isolated_tenant, dummy_mock, run_worker, wait_for_completion, WORKER_ADDRESS


async def test_policy_and_metrics_use_latest_epoch_not_stale_history(cp):
    mode = {"v": "fail"}
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("epoch-isolation-wf", version=1)
    async def v1(ctx):
        if mode["v"] == "fail":
            ctx.mark_failed()
        return Complete()

    @worker.workflow("epoch-isolation-wf", version=2)
    async def v2(ctx):
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "epoch-isolation")
        wf = await cp.create_workflow("epoch-isolation-wf", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)

        # Drive v1's first epoch to 2 failures -- this is the stale data that must
        # not leak into the fresh epoch created below.
        for _ in range(2):
            request_id = await cp.invoke_workflow(wf.id)
            await wait_for_completion(cp, request_id)
        stale_epoch_metrics = await cp.get_workflow_metrics(wf.id)
        assert stale_epoch_metrics.total_failures == 2

        # Redeploy away and back: v1's second arrival starts a fresh epoch.
        await cp.set_workflow_config(wf.id, WorkflowConfig(version=2))
        await cp.set_workflow_config(wf.id, WorkflowConfig(version=1))
        fresh_epoch_metrics = await cp.get_workflow_metrics(wf.id)
        assert fresh_epoch_metrics.total_failures == 0, (
            "a fresh epoch must not inherit the prior epoch's failure count"
        )

        # Arm a rule at the SAME threshold the stale epoch already exceeded (2). If
        # decisions were reading stale epoch-1 data, this would misfire on the very
        # next completed run, before the fresh epoch ever reaches 2 failures itself.
        await cp.set_workflow_lifecycle_policy(wf.id, [
            WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=2, action=SetVersion(target=2)),
        ])

        request_id = await cp.invoke_workflow(wf.id)
        await wait_for_completion(cp, request_id)
        after_one_failure = await cp.get_workflow_metrics(wf.id)
        assert after_one_failure.total_failures == 1
        assert after_one_failure.version == 1, (
            "rule must not fire on the fresh epoch's first failure -- if it did, "
            "it was evaluated against the stale epoch's already-at-threshold count"
        )

        # A second failure on the fresh epoch genuinely reaches the threshold --
        # confirms rule evaluation is live, not just silently never firing.
        request_id = await cp.invoke_workflow(wf.id)
        await wait_for_completion(cp, request_id)
        final = await cp.get_workflow(wf.id)
        assert final.version == 2, "rule must fire once the fresh epoch itself reaches the threshold"
