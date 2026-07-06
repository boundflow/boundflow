"""LangGraph *inside* a governed BoundFlow workflow.

LangGraph is an in-process library for authoring stateful agent graphs — nodes,
conditional edges, loops. BoundFlow is the governed runtime around the workflow.
They compose cleanly when the graph lives inside the workflow handler and its nodes
run their reasoning through `ctx.run_agent`: LangGraph owns the control flow
(routing, branching) while BoundFlow governs each agent step (cost caps, LLM-call
limits, model policies, metrics, traces) and the workflow as a whole (versioning,
approvals, rollback).

The graph here triages a topic, then either researches-then-writes or writes
directly — one conditional branch, so you can see LangGraph routing between two
governed agent nodes. Each node feeds its agent through `ctx.run_agent`, so every
model call is under governance; nothing escapes to a raw model.

Key detail: thread `ctx` into the nodes via a closure (build the graph per run),
not through the graph state — `ctx` is a live object, and graph state should stay
plain data (and serializable if you ever add a checkpointer).

Prerequisites: a running backend (`docker compose up -d`) and:
    pip install "boundflow" langgraph
    export BOUNDFLOW_API_KEY=<from: docker compose run --rm server -mode=provision -name=me>
    export ANTHROPIC_API_KEY=<your Anthropic key>

Run:
    python -m boundflow.examples.langgraph_workflow
"""
import asyncio
import os
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    ControlPlaneClient,
    WorkflowConfig,
)
from boundflow.anthropic_client import AnthropicLlmClient

MODEL = "claude-haiku-4-5"

triager = AgentDefinition(
    name="triager",
    system_prompt="Decide whether the topic needs research or can be answered directly. "
                  "Answer 'deep' if it needs current facts or specifics, else 'quick'.",
    model=MODEL,
    output_schema={"route": {"type": "string", "enum": ["deep", "quick"]}},
)
researcher = AgentDefinition(
    name="researcher",
    system_prompt="Produce 3-4 concise factual notes a writer could use on the topic.",
    model=MODEL,
    output_schema={"notes": {"type": "string"}},
)
writer = AgentDefinition(
    name="writer",
    system_prompt="Write one short paragraph on the topic, using any notes provided.",
    model=MODEL,
    output_schema={"article": {"type": "string"}},
)


class State(TypedDict, total=False):
    topic: str
    route: str
    article: str


def build_graph(ctx):
    """Compile a graph whose nodes close over the BoundFlow ctx, so each runs its
    agent under governance. The topic is added to the operation context once up front."""

    async def triage(state: State) -> State:
        out = await ctx.run_agent(triager)                 # governed agent call
        return {"route": out.output["route"]}

    async def research(state: State) -> State:
        out = await ctx.run_agent(researcher)              # governed agent call
        ctx.add_context("notes", out.output["notes"])      # feed the writer downstream
        return {}

    async def write(state: State) -> State:
        out = await ctx.run_agent(writer)                  # governed agent call
        return {"article": out.output["article"]}

    g = StateGraph(State)
    g.add_node("triage", triage)
    g.add_node("research", research)
    g.add_node("write", write)
    g.add_edge(START, "triage")
    # LangGraph does the routing; BoundFlow governs whatever each branch runs.
    g.add_conditional_edges("triage", lambda s: s["route"],
                            {"deep": "research", "quick": "write"})
    g.add_edge("research", "write")
    g.add_edge("write", END)
    return g.compile()


async def main() -> None:
    worker = BoundFlowWorker("http://localhost:50052",
                             AnthropicLlmClient(os.environ["ANTHROPIC_API_KEY"]))

    @worker.workflow("langgraph_pipeline", version=1)
    async def pipeline(ctx):
        ctx.add_context("topic", "the James Webb Space Telescope's first deep-field image")
        graph = build_graph(ctx)                           # the graph lives in the workflow
        final = await graph.ainvoke({})
        print("  route taken:", final["route"])
        print("  article:", final["article"])
        return Complete()

    worker_task = asyncio.create_task(worker.run())
    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant("langgraph")
        wf = await cp.create_workflow("langgraph_pipeline", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)
        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)
        while not (await cp.get_request_info(request_id)).status.is_terminal():
            await asyncio.sleep(0.5)
        print("  done:", (await cp.get_request_info(request_id)).status.value)

    worker_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
