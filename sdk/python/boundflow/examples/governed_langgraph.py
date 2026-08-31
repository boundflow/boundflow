"""A LangGraph agent that BoundFlow governs but doesn't drive.

The sibling example (`langgraph_workflow`) has LangGraph route *between* BoundFlow
agent nodes — the graph picks the path, but `ctx.run_agent` still owns each agent
loop. This is the inverse, and it's what you want when the agent itself is
LangGraph-shaped: a real ReAct loop with a tool node and message state, where
LangGraph owns the loop and BoundFlow governs every model call underneath.

    model = ctx.agent_model("researcher", ChatAnthropic(model=MODEL))
    tools = ctx.agent_tools("researcher", [word_count])
    agent = create_react_agent(model, tools)
    await agent.ainvoke({"messages": [...]})

Hand BoundFlow the model you want governed and the tools you want governed; it
governs those and stays out of everything else. Everything the graph does still
lands on the run's receipt — cost, tokens, LLM *and* tool spans, per-agent metrics
for lifecycle policy — and the agent's runtime policy still applies, including
per-tool caps for the tools you passed through `agent_tools()`.

The agent below gets a tiny `max_llm_calls`, so a loop that keeps calling its tool
is stopped by policy rather than by luck.

What you still give up versus `run_agent`: hitting an LLM/cost cap raises rather
than asking the model for a graceful final answer. See `boundflow.governed`.

Prerequisites: a running backend (`docker compose up -d`) and:
    pip install "boundflow[langchain]" langgraph langchain-anthropic
    export BOUNDFLOW_API_KEY=<from: docker compose run --rm server -mode=provision -name=me>
    export ANTHROPIC_API_KEY=<your Anthropic key>

Run:
    python -m boundflow.examples.governed_langgraph
"""
import asyncio
import os

from langchain_anthropic import ChatAnthropic
from langchain_core.tools import tool
# Moves to `langchain.agents.create_agent` in LangGraph v2; this import works today
# with just `pip install langgraph`.
from langgraph.prebuilt import create_react_agent

from boundflow import (
    AgentPolicyLimitExceeded,
    BoundFlowWorker,
    Complete,
    ControlPlaneClient,
    RuntimePolicy,
    WorkflowConfig,
)
from boundflow.anthropic_client import AnthropicLlmClient

MODEL = "claude-haiku-4-5"
AGENT = "researcher"
WORKFLOW = "governed_langgraph"


@tool
def word_count(text: str) -> int:
    """Count the words in a piece of text."""
    return len(text.split())


async def main() -> None:
    api_key = os.environ["ANTHROPIC_API_KEY"]
    worker = BoundFlowWorker(llm=AnthropicLlmClient(api_key))

    @worker.workflow(WORKFLOW, version=1)
    async def _entry(ctx):
        # A governed BaseChatModel. LangGraph drives it; BoundFlow meters it and
        # enforces the agent's runtime policy on every call.
        model = ctx.agent_model(AGENT, ChatAnthropic(model=MODEL, api_key=api_key))
        # Governed too: per-tool caps apply, failures are counted, tool spans recorded.
        tools = ctx.agent_tools(AGENT, [word_count])
        agent = create_react_agent(model, tools)

        try:
            result = await agent.ainvoke({"messages": [
                ("user", "How many words are in the sentence 'BoundFlow governs agents'? "
                         "Use the tool, then answer."),
            ]})
            answer = result["messages"][-1].content
            return Complete(result={"answer": answer})
        except AgentPolicyLimitExceeded as exc:
            # The cap tripped mid-graph. The run is recorded as failed and the spend
            # it burned is still on the receipt.
            return Complete(result={"stopped_by_policy": str(exc)})

    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant("governed-langgraph-demo")
        wf = await cp.create_workflow(WORKFLOW, tenant.id, config=WorkflowConfig(version=1))
        try:
            # The cap lives server-side, not in the handler — the graph can't opt out.
            await cp.set_agent_runtime_policy(wf.id, AGENT, RuntimePolicy(max_llm_calls=4))
            await cp.activate_workflow(wf.id)

            task = asyncio.create_task(worker.run())
            await asyncio.sleep(0.5)
            try:
                request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)
                while True:
                    info = await cp.get_request_info(request_id)
                    if info.status in ("completed", "failed"):
                        break
                    await asyncio.sleep(1)
                print(f"run {info.status}: {info.result}")

                metrics = await cp.get_workflow_metrics(wf.id)
                print(f"governed calls: {metrics.total_llm_calls}  "
                      f"cost: ${metrics.total_cost_usd:.6f}")
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        finally:
            await cp.delete_workflow(wf.id)
            await cp.delete_tenant(tenant.id)


if __name__ == "__main__":
    asyncio.run(main())
