"""The governance audit log — approvals + workflow-policy + agent-policy in one view.

Runs five workflows under one tenant, each producing a different audit event, then
prints the unified, time-ordered log from a single `cp.get_audit_log()` call:

  - two approval decisions (approved / rejected, with the actor)
  - a workflow-lifecycle policy firing (cooldown) and another (pause)
  - an agent-lifecycle policy firing (SetModel) — base -> effective + why

The decisions are the BoundFlow system-of-record (server-side), distinct from the
execution telemetry traces. Per-type getters also exist:
`get_approval_audit(workflow_id)`, `get_approval_audit_by_id(approval_id)`,
`get_workflow_policy_audit(workflow_id)`, `get_agent_policy_audit(workflow_id, agent)`.

Prereqs
-------
1. Backend up (repo root):  docker compose up -d
2. Provision a key:         docker compose run --rm server -mode=provision -name=audit-demo
                            export BOUNDFLOW_API_KEY=<the printed api_key>
3. Run:                     python unified_audit_log.py     (mock LLM — no Anthropic key needed)
"""
from __future__ import annotations

import asyncio
import os

from boundflow import (
    AgentDefinition, AgentMetric, AgentPolicyActionRecord, AgentRule, ApprovalAuditRecord,
    ApprovalRequest, AwaitApproval, BoundFlowWorker, Complete, ControlPlaneClient,
    Cooldown, LifecycleState, MockLlmClient, Op, Pause, PolicyActionRecord, SetModel,
    WorkflowConfig, WorkflowMetric, WorkflowRule, WorkflowState, submit,
)

WORKER = "http://localhost:50052"
SERVER = "http://localhost:50051"
SONNET = "claude-sonnet-4-6"
HAIKU = "claude-haiku-4-5-20251001"
approvals: dict[str, str] = {}


async def wait_lifecycle(cp, wid, target):
    while await cp.get_workflow_lifecycle_state(wid) != target:
        await asyncio.sleep(0.3)


async def wait_wf_state(cp, wid, target):
    while await cp.get_workflow_state(wid) != target:
        await asyncio.sleep(0.3)


async def wait_active(cp, wid):
    while await cp.get_workflow_lifecycle_state(wid) == LifecycleState.INVOKING:
        await asyncio.sleep(0.3)


async def main():
    key = os.environ["BOUNDFLOW_API_KEY"]
    worker = BoundFlowWorker(WORKER, MockLlmClient(lambda _: submit()), api_key=key)

    @worker.workflow("expense-approval", version=1)
    async def _expense(ctx):
        return AwaitApproval(on_approve=Complete(), on_reject=Complete(), timeout=600,
                             justification="approve expense over $5k")

    @worker.workflow("data-ingest", version=1)
    async def _ingest(ctx):
        ctx.mark_failed()
        return Complete()

    @worker.workflow("model-deploy", version=1)
    async def _deploy(ctx):
        ctx.mark_failed()
        return Complete()

    @worker.workflow("model-router", version=1)
    async def _router(ctx):
        await ctx.run_agent(AgentDefinition(name="router", system_prompt="x", model=SONNET,
                                            output_schema={"done": {"type": "boolean"}}))
        return Complete()

    @worker.on_approval_requested
    async def _on(req: ApprovalRequest):
        approvals[req.workflow_id] = req.approval_id

    wt = asyncio.create_task(worker.run())
    await asyncio.sleep(0.2)

    async with ControlPlaneClient(SERVER, api_key=key) as cp:
        tenant = await cp.create_tenant("acme-ops")

        async def approval(decide, actor):
            wf = await cp.create_workflow("expense-approval", tenant.id, WorkflowConfig(version=1))
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=600)
            await wait_lifecycle(cp, wf.id, LifecycleState.AWAITING_APPROVAL)
            aid = approvals[wf.id]
            await (cp.approve_workflow if decide == "approve" else cp.reject_workflow)(wf.id, aid, actor=actor)

        async def wf_policy(wf_type, action, target):
            wf = await cp.create_workflow(wf_type, tenant.id, WorkflowConfig(version=1))
            await cp.set_workflow_lifecycle_policy(wf.id, [
                WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=1, action=action)])
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_wf_state(cp, wf.id, target)

        print("running workflows under tenant 'acme-ops' ...")
        await approval("approve", "alice@acme.com")
        await approval("reject", "bob@acme.com")
        await wf_policy("data-ingest", Cooldown(window=1, seconds=30), WorkflowState.COOLDOWN)
        await wf_policy("model-deploy", Pause(window=1), WorkflowState.PAUSED)

        # agent lifecycle: SetModel fires on the 2nd run (llm_calls>=1 from run 1)
        wf = await cp.create_workflow("model-router", tenant.id, WorkflowConfig(version=1))
        await cp.set_agent_lifecycle_policy(wf.id, "router", [
            AgentRule(metric=AgentMetric.LLM_CALLS, op=Op.GTE, threshold=1, window=1,
                      action=SetModel(value=HAIKU))])
        await cp.activate_workflow(wf.id)
        for _ in range(2):
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_active(cp, wf.id)

        await asyncio.sleep(2)  # let the scheduler/worker write the async policy rows

        print("\n==================== UNIFIED AUDIT LOG (tenant acme-ops) ====================")
        for r in await cp.get_audit_log():
            if isinstance(r, ApprovalAuditRecord):
                print(f"  approval  | {r.decision:9} by {r.actor:16} wf={r.workflow_id[:8]}")
            elif isinstance(r, PolicyActionRecord):
                extra = f"{r.cooldown_seconds}s" if r.action == "cooldown" else ""
                print(f"  workflow  | {r.action:9}{extra:5} {r.metric}>={r.threshold:g} {r.previous_state}->{r.action} wf={r.workflow_id[:8]}")
            elif isinstance(r, AgentPolicyActionRecord):
                fr = r.fired_rules[0]
                act = fr["action"]
                print(f"  agent     | {r.agent}: {act['field']} {r.base_policy['model'] or '∅'}->{act['value']} ({fr['metric']}>={fr['threshold']:g}) wf={r.workflow_id[:8]}")
        print()

    wt.cancel()
    await asyncio.gather(wt, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
