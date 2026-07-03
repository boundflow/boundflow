"""Policy-action audit: when a workflow lifecycle policy fires and changes state,
a self-describing audit row is recorded (the rule that fired + the value that
crossed + the prior state), fetchable via GetPolicyAudit. No telemetry involved —
this is governance system-of-record, written server-side by the scheduler.
"""
from __future__ import annotations

import asyncio

from boundflow import (
    BoundFlowWorker,
    Complete,
    Cooldown,
    Pause,
    WorkflowConfig,
    WorkflowMetric,
    WorkflowRule,
    WorkflowState,
)

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_workflow_state,
)


async def _wait_for_policy_audit(cp, workflow_id: str, timeout: int = 15):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        records = await cp.get_workflow_policy_audit(workflow_id=workflow_id)
        if records:
            return records
        assert asyncio.get_event_loop().time() < deadline, f"policy audit for {workflow_id} never appeared"
        await asyncio.sleep(0.3)


async def test_cooldown_policy_writes_self_describing_audit(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("policy_cooldown", version=1)
    async def _entry(ctx):
        ctx.mark_failed()
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "policy-cooldown")
        wf = await cp.create_workflow("policy_cooldown", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_workflow_lifecycle_policy(wf.id, [
                WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=1,
                             action=Cooldown(window=1, seconds=30)),
            ])
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_workflow_state(cp, wf.id, WorkflowState.COOLDOWN)

            records = await _wait_for_policy_audit(cp, wf.id)
            assert len(records) == 1
            r = records[0]
            assert r.action == "cooldown"
            assert r.cooldown_seconds == 30
            # The fired rule, echoed back (identified by content).
            assert r.metric == "num_failures"
            assert r.threshold == 1
            assert r.trigger_value >= 1
            # The transition + provenance.
            assert r.previous_state == "active"
            assert r.previous_version == 1
            assert r.actor == "system"
            assert r.occurred_at is not None
        finally:
            await cp.delete_workflow(wf.id)


async def test_pause_policy_writes_audit(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("policy_pause", version=1)
    async def _entry(ctx):
        ctx.mark_failed()
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "policy-pause")
        wf = await cp.create_workflow("policy_pause", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_workflow_lifecycle_policy(wf.id, [
                WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=1,
                             action=Pause(window=1)),
            ])
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_workflow_state(cp, wf.id, WorkflowState.PAUSED)

            records = await _wait_for_policy_audit(cp, wf.id)
            assert records[0].action == "pause"
            assert records[0].metric == "num_failures"
            assert records[0].previous_state == "active"
            assert records[0].actor == "system"
        finally:
            await cp.delete_workflow(wf.id)
