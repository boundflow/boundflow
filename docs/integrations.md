# Integrations: LangChain & LangGraph

BoundFlow governs the **runtime** — versioning, cost caps, approvals, metrics, rollback. It composes with the LangChain ecosystem at two different layers:

- **LangChain** supplies the *model* — use any provider LangChain can talk to.
- **LangGraph** supplies *orchestration* — author a stateful agent graph that runs *inside* a governed workflow.

The rule that governs both: **BoundFlow governs the model calls that flow through its agent loop (`ctx.run_agent`).** Anything that calls a raw model directly runs outside that boundary.

## Any provider via LangChain

`LangChainLlmClient` wraps any tool-calling LangChain chat model, so a BoundFlow agent runs on any provider (Anthropic, OpenAI, Google, Bedrock, …) with governance unchanged:

```python
from langchain_anthropic import ChatAnthropic          # or ChatOpenAI, ChatVertexAI, ...
from boundflow import BoundFlowWorker
from boundflow.langchain_client import LangChainLlmClient

worker = BoundFlowWorker(llm=LangChainLlmClient(ChatAnthropic(model="claude-haiku-4-5")))
```

Pass a factory — `LangChainLlmClient(lambda name: ChatAnthropic(model=name))` — to let `SetModel` policies choose the model per run. Install with `pip install "boundflow[langchain]"`.

!!! note "The model must support tool calling and report usage"
    BoundFlow drives tools plus a `submit_result` tool for structured output, so a
    non-tool-calling model can't complete a run. Token usage is what cost governance is
    priced on: if a provider reports **no** usage, the run fails loud (a platform failure
    that interrupts the workflow) rather than running uncosted and escaping its cost caps.

Runnable example: [`boundflow.examples.langchain_adapter`](https://github.com/boundflow/boundflow/blob/main/sdk/python/boundflow/examples/langchain_adapter.py).

## LangGraph inside a governed workflow

LangGraph is an in-process library for authoring stateful agent graphs — nodes, conditional edges, loops. It runs when you call `graph.ainvoke(...)`, so it lives happily **inside** a BoundFlow workflow handler. The composition that keeps governance intact: the graph's nodes run their reasoning through `ctx.run_agent`, so LangGraph owns the control flow while BoundFlow governs each agent step *and* the workflow as a whole.

Thread `ctx` into the nodes with a **closure** (build the graph per run) — `ctx` is a live object, so keep it out of the graph state (state should stay plain data).

```python
from typing import TypedDict
from langgraph.graph import END, START, StateGraph

class State(TypedDict, total=False):
    route: str
    article: str

def build_graph(ctx):
    async def triage(state):
        out = await ctx.run_agent(triager)             # governed agent call
        return {"route": out.output["route"]}

    async def research(state):
        out = await ctx.run_agent(researcher)          # governed agent call
        ctx.add_context("notes", out.output["notes"])  # feed the writer downstream
        return {}

    async def write(state):
        out = await ctx.run_agent(writer)              # governed agent call
        return {"article": out.output["article"]}

    g = StateGraph(State)
    g.add_node("triage", triage); g.add_node("research", research); g.add_node("write", write)
    g.add_edge(START, "triage")
    # LangGraph does the routing; BoundFlow governs whatever each branch runs.
    g.add_conditional_edges("triage", lambda s: s["route"],
                            {"deep": "research", "quick": "write"})
    g.add_edge("research", "write"); g.add_edge("write", END)
    return g.compile()

@worker.workflow("langgraph_pipeline", version=1)
async def pipeline(ctx):
    ctx.add_context("topic", "...")
    graph = build_graph(ctx)          # the graph lives in the workflow
    final = await graph.ainvoke({})
    return Complete()
```

Every reasoning step goes through `ctx.run_agent`, so each is cost-capped, call-limited, metered, and traced — and the whole pipeline is one versioned, rollback-able workflow. LangGraph contributes exactly what it's best at: the branching and state machine *between* governed steps.

!!! warning "The boundary is per node"
    Governance applies to calls made through `ctx.run_agent`. If a node calls a raw
    LangChain model directly, *that* call is invisible to BoundFlow — its cost and token
    use never enter the ledger, so cost-based policies can't see it. Route reasoning
    through `ctx.run_agent` to keep it governed.

!!! note "`add_context` accumulates for the whole operation"
    Context added via `ctx.add_context` is not cleared between `ctx.run_agent` calls —
    it builds up across the operation. Add each distinct input once (as the example
    does) rather than re-adding the same key in a loop.

Runnable example: [`boundflow.examples.langgraph_workflow`](https://github.com/boundflow/boundflow/blob/main/sdk/python/boundflow/examples/langgraph_workflow.py).

### When to reach the other way (BoundFlow agent as a tool)

If your app is *already* a LangGraph/LangChain agent and you only want to pull in one governed BoundFlow agent as a step, you can invoke a workflow from a node with the [`ControlPlaneClient`](api-reference.md) (`invoke_workflow` → poll `get_request_info` → branch on `run_outcome`). Note that invoking a workflow is fire-and-observe: it returns the run's **outcome**, not the agent's output payload — so this fits governed *actions* (gated, cost-capped side effects), not returning a generated value back into the graph. For "run my graph under governance," prefer LangGraph *inside* the workflow, above.
