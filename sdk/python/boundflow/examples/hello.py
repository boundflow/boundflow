"""Hello, BoundFlow — register a workflow, run a real agent, see the result.

Prerequisites: a running backend (`docker compose up -d`) and:
    export BOUNDFLOW_API_KEY=<from: docker compose run --rm server -mode=provision -name=me>
    export ANTHROPIC_API_KEY=<your Anthropic key>     # the agent makes a real call

Run:
    python -m boundflow.examples.hello
"""
import asyncio
import os

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    ControlPlaneClient,
    LifecycleState,
    WorkflowConfig,
)
from boundflow.anthropic_client import AnthropicLlmClient


async def main() -> None:
    # Endpoints default to localhost; BOUNDFLOW_API_KEY comes from the env.
    llm = AnthropicLlmClient(os.environ["ANTHROPIC_API_KEY"])
    worker = BoundFlowWorker(llm=llm)

    summarizer = AgentDefinition(
        name="summarizer",
        system_prompt="You summarize text in one short, plain sentence.",
        model="claude-haiku-4-5",
        output_schema={"summary": {"type": "string"}},
    )

    @worker.workflow("hello", version=1)
    async def hello(ctx):
        ctx.add_context(
            "text",
            "BoundFlow runs fleets of agents under governance: cost caps, "
            "model policies, and human approval gates.",
        )
        result = await ctx.run_agent(summarizer)
        print("  agent summary:", result.output["summary"])
        return Complete()

    worker_task = asyncio.create_task(worker.run())
    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant("hello")
        wf = await cp.create_workflow("hello", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)

        while await cp.get_workflow_lifecycle_state(wf.id) == LifecycleState.INVOKING:
            await asyncio.sleep(0.5)
        print("  done:", await cp.get_workflow_lifecycle_state(wf.id))

    worker_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
