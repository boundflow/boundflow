"""Agent lifecycle policy — adapt the model based on recent runs.

After each run, agent-lifecycle rules evaluate the agent's recent metrics and can
switch its model. Here the analyst starts on Sonnet; once its first run has made
an LLM call, the rule downgrades it to Haiku for the next run. Watch `model_used`
change between run 1 and run 2.

Deterministic — no Anthropic key needed. Prereqs: backend up + BOUNDFLOW_API_KEY.
Run:  python -m boundflow.examples.model_switching
"""
import asyncio

from boundflow import (
    AgentDefinition, AgentMetric, AgentRule, BoundFlowWorker, Complete,
    ControlPlaneClient, MockLlmClient, Op, SetModel, WorkflowConfig, submit,
)

AGENT = "analyst"
SONNET, HAIKU = "claude-sonnet-4-6", "claude-haiku-4-5"


async def main() -> None:
    # A mock that makes one LLM call and submits — enough to produce the metric
    # (llm_calls >= 1) the rule keys on. The model is still chosen by the policy.
    worker = BoundFlowWorker(llm=MockLlmClient(lambda _: submit()))

    @worker.workflow("adaptive", version=1)
    async def _entry(ctx):
        result = await ctx.run_agent(AgentDefinition(
            name=AGENT, system_prompt="You are a concise analyst.",
            model=SONNET, output_schema={"summary": {"type": "string"}},
        ))
        print(f"  ran on: {result.model_used}")
        return Complete()

    task = asyncio.create_task(worker.run())
    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant("model-switching")
        wf = await cp.create_workflow("adaptive", tenant.id, config=WorkflowConfig(version=1))
        try:
            # After a run makes >= 1 LLM call, downgrade the model for the next run.
            await cp.set_agent_lifecycle_policy(wf.id, AGENT, [
                AgentRule(metric=AgentMetric.LLM_CALLS, op=Op.GTE, threshold=1,
                          window=1, action=SetModel(value=HAIKU)),
            ])
            await cp.activate_workflow(wf.id)

            print("run 1 (no history yet):")
            await _wait_done(cp, await cp.invoke_workflow(wf.id, operation_timeout_seconds=30))
            print("run 2 (rule fires on run-1's metrics):")
            await _wait_done(cp, await cp.invoke_workflow(wf.id, operation_timeout_seconds=30))
            print("  → the agent auto-downgraded from Sonnet to Haiku.")
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


if __name__ == "__main__":
    asyncio.run(main())
