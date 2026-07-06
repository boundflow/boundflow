"""Run a BoundFlow agent through a LangChain chat model.

Same governed agent as `hello`, but the LLM calls go through LangChain's provider
ecosystem via `LangChainLlmClient` — swap `ChatAnthropic` for any tool-calling
LangChain chat model (OpenAI, Google, Bedrock, ...) and the governance is identical.

Prerequisites: a running backend (`docker compose up -d`), the LangChain extra, and:
    pip install "boundflow[langchain]" langchain-anthropic
    export BOUNDFLOW_API_KEY=<from: docker compose run --rm server -mode=provision -name=me>
    export ANTHROPIC_API_KEY=<your Anthropic key>     # the agent makes a real call

Run:
    python -m boundflow.examples.langchain_adapter
"""
import asyncio
import os

from langchain_anthropic import ChatAnthropic

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    ControlPlaneClient,
    WorkflowConfig,
)
from boundflow.langchain_client import LangChainLlmClient


async def main() -> None:
    # Any tool-calling LangChain chat model works here; BoundFlow governs it the same.
    # Pass `lambda name: ChatAnthropic(model=name)` instead to let SetModel policies pick.
    model = ChatAnthropic(model="claude-haiku-4-5", api_key=os.environ["ANTHROPIC_API_KEY"])
    worker = BoundFlowWorker(llm=LangChainLlmClient(model))

    summarizer = AgentDefinition(
        name="summarizer",
        system_prompt="You summarize text in one short, plain sentence.",
        model="claude-haiku-4-5",  # priced against this name for cost governance
        output_schema={"summary": {"type": "string"}},
    )

    @worker.workflow("langchain_hello", version=1)
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
        tenant = await cp.create_tenant("langchain-hello")
        wf = await cp.create_workflow("langchain_hello", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)
        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)

        while not (await cp.get_request_info(request_id)).status.is_terminal():
            await asyncio.sleep(0.5)
        print("  done:", (await cp.get_request_info(request_id)).status.value)

    worker_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
