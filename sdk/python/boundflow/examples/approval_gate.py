"""Human-in-the-loop: pause for approval before a sensitive action.

A workflow runs a real agent, then *parks* awaiting a human decision — nothing
irreversible happens until someone approves. This is BoundFlow's approval gate.

Prerequisites: a running backend and:
    export BOUNDFLOW_API_KEY=...    export ANTHROPIC_API_KEY=...

Run:
    python -m boundflow.examples.approval_gate
"""
import asyncio
import os

from boundflow import (
    AgentDefinition,
    ApprovalRequest,
    AwaitApproval,
    BoundFlowWorker,
    Complete,
    ControlPlaneClient,
    LifecycleState,
    Next,
    WorkflowConfig,
)
from boundflow.anthropic_client import AnthropicLlmClient


async def main() -> None:
    llm = AnthropicLlmClient(os.environ["ANTHROPIC_API_KEY"])
    worker = BoundFlowWorker(llm=llm)
    pending: list[ApprovalRequest] = []

    analyst = AgentDefinition(
        name="analyst",
        system_prompt="You review a refund request and recommend an action in one sentence.",
        model="claude-haiku-4-5",
        output_schema={"recommendation": {"type": "string"}},
    )

    @worker.workflow("refund", version=1)
    async def refund(ctx):
        ctx.add_context("request", "Customer wants a $5,000 refund, 40 days after purchase.")
        result = await ctx.run_agent(analyst)
        print("  analyst:", result.output["recommendation"])
        # Don't issue the refund until a human signs off.
        return AwaitApproval(
            on_approve=Next("issue_refund", ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=120,
            justification="Approve the $5,000 refund?",
        )

    @worker.operation("refund", "issue_refund")
    async def issue_refund(ctx):
        print("  ✅ refund issued (human approved)")
        return Complete()

    @worker.on_approval_requested
    async def on_approval(req: ApprovalRequest):
        pending.append(req)

    worker_task = asyncio.create_task(worker.run())
    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant("approval")
        wf = await cp.create_workflow("refund", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)

        # Wait for the workflow to park awaiting a decision.
        while not pending:
            await asyncio.sleep(0.5)
        req = pending[0]
        print(f"  ⏳ awaiting approval: {req.justification}")

        # A human decides. Approve here; swap for cp.reject_workflow(...) to see
        # the on_reject branch (the refund is never issued).
        await cp.approve_workflow(wf.id, req.approval_id)
        while (await cp.get_workflow(wf.id)).lifecycle_state != LifecycleState.ACTIVE:
            await asyncio.sleep(0.5)
        print("  done")

    worker_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
