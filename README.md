# BoundFlow

**The operational layer for the LLM agents and workflows you run unattended — cost caps, approval gates, and self-healing policy, enforced by a control plane.**

![status: preview](https://img.shields.io/badge/status-preview-orange)
![backend: Apache-2.0](https://img.shields.io/badge/backend-Apache--2.0-blue)
![SDK: MIT](https://img.shields.io/badge/SDK-MIT-blue)

> [!IMPORTANT]
> **Public preview (pre-1.0).** The engine is complete and covered by Go, mock-LLM,
> and live-LLM test suites, but it hasn't yet been run in production with external
> users. APIs — including the gRPC protobufs — may change before 1.0. We're looking
> for early adopters and design partners: [reach out](mailto:arjunvlama1@gmail.com).

BoundFlow runs long-running, stateful agent workflows and enforces the guardrails
you'll want before running agents unattended: per-run **cost caps**, automatic **model switching** on
cost/loop policies, **human approval gates** before sensitive actions, tool-call
limits, retries, cooldowns, and versioned rollbacks. You write agents and
workflows against a clean async SDK; the control plane schedules, dispatches, and
governs them.

Inference is **bring-your-own** — your agents call Claude with your own Anthropic
key, running in your worker. The backend never sees it and never pays for tokens.
Your keys, your data, and your token spend stay on your side of the wire.

**In practice:** a support-triage workflow that may spend up to **$0.25/run**, must
get a **human's sign-off** before issuing a refund, **downgrades to Haiku** when
costs spike, and **auto-rolls-back** to the last good version if it starts
failing — none of that logic living in your agent code. You declare it as policy;
the control plane enforces it and keeps a durable, queryable **audit log** of every
approval and policy decision.

BoundFlow is *not* a prompt framework, an inference provider, or an agent-builder —
it's the operational layer *around* the agents you build.

- **Backend** — open source (Apache-2.0), self-hostable as a container.
- **Python SDK** — open source (MIT), `pip install boundflow`.
- **Docs** — concepts, governance, deployment, and API reference in [`docs/`](docs/).
- **BoundFlow Cloud** — prefer not to self-host? Managed hosting (early access) — see [below](#hosted-boundflow-cloud).

---

## Quick start

Get a governed agent running in a few minutes. Full walkthrough: **[QUICKSTART.md](QUICKSTART.md)**.

```bash
# 1. Start the backend (Postgres + server + scheduler + worker)
docker compose -f docker-compose.dist.yml up -d

# 2. Provision an API key
docker compose -f docker-compose.dist.yml run --rm server -mode=provision -name=me
export BOUNDFLOW_API_KEY=<printed key>

# 3. Install the SDK and bring your Anthropic key
pip install boundflow
export ANTHROPIC_API_KEY=<your key>

# 4. Run a real agent under governance
python -m boundflow.examples.hello
```

Then explore the bundled examples:

```bash
python -m boundflow.examples.approval_gate   # human-in-the-loop sign-off
```

---

## Why BoundFlow

**Agents that take real actions need a control plane that takes real action when
they go wrong.** Most tools *watch* your agents; BoundFlow *intervenes* — at both
levels. On the **agent**: cap its spend, swap its model mid-run. On the
**workflow**: gate a risky step for human sign-off, cool it down, roll it back to a
known-good version, or pause it outright. It's **workflow-aware, not just
agent-aware** — because it runs the whole workflow, not just the model call:
scheduling each run, carrying state across steps, recovering from failures, and
driving it through its lifecycle, with the agent as just one operation inside a
durable, multi-step process it owns end to end.

The moment agents run unattended you need answers to: *What if it loops? What if
it spends $50? What if it's about to do something irreversible? Which model should
it use, and when should that change?* BoundFlow makes those **policies** instead of
code:

| Concern | BoundFlow gives you |
|---|---|
| Runaway spend | `max_cost_usd` runtime cap — halts the agent when a run's cost crosses a budget |
| Wrong model for the job | Lifecycle policies that switch the model on cost/loop signals (e.g. downgrade to Haiku after a cost spike) |
| Irreversible actions | Approval gates — the workflow parks for a human decision before continuing |
| Output blowups | `max_tokens_per_call`, per-tool call limits |
| Flaky/failing runs | Cooldowns, automatic version rollback |
| Cost accounting | Per-tenant model pricing, cache-aware cost from real token usage |
| Flying blind | OpenTelemetry-native traces of every run (GenAI semantics) shipped to *your* stack — Jaeger, Tempo, Langfuse, Phoenix — plus a durable, queryable audit log of every approval and policy decision |

Policies are evaluated server-side (lifecycle) and enforced SDK-side (runtime),
with per-invocation metrics — cost, tokens, LLM calls, per-tool counts/failures —
collected on every run.

---

## Architecture

The **BoundFlow backend** is the control plane — self-host it, or run it on
BoundFlow Cloud. Either way, your **worker** connects to it over gRPC and runs the
actual agents, with your Anthropic key, in your environment; the backend schedules,
dispatches, governs, and audits, and never sees your key or your inference traffic.

```
   ┌─────────────────────┐      gRPC        ┌────────────────────────┐
   │  Your client / SDK  │ ───────────────▶ │                        │
   └─────────────────────┘  invoke·approve  │   BoundFlow backend    │
                             ·query          │   (control plane)      │
   ┌─────────────────────┐   gRPC stream    │                        │
   │  Your worker        │ ◀──────────────▶ │  schedules·dispatches  │
   │  runs agents+tools  │  launch/result   │  ·governs·audits       │
   │  with your API key  │                  └────────────────────────┘
   └─────────────────────┘
```

Under the hood the backend runs as three process modes (`server`, `scheduler`,
`worker`) off one binary sharing Postgres — see
**[docs/concepts.md](docs/concepts.md)** for the full breakdown and the lifecycle
states.

---

## SDK at a glance

```python
from boundflow import AgentDefinition, BoundFlowWorker, Complete, ControlPlaneClient, WorkflowConfig
from boundflow.anthropic_client import AnthropicLlmClient

worker = BoundFlowWorker(llm=AnthropicLlmClient(...))  # endpoints + key from env

@worker.workflow("triage", version=1)
async def triage(ctx):
    ctx.add_context("ticket", "...")
    await ctx.run_agent(AgentDefinition(
        name="analyst", model="claude-haiku-4-5",
        system_prompt="Diagnose the issue.", output_schema={"summary": {"type": "string"}},
    ))
    return Complete()
```

Workflows are **multi-step and stateful**: an operation can park for a human
decision or chain into a follow-on operation, and the workflow resumes where it
left off — nothing irreversible runs until the branch it's gated behind does.

```python
from boundflow import AwaitApproval, Next, Complete

@worker.workflow("refund", version=1)
async def refund(ctx):
    await ctx.run_agent(analyst)                    # step 1: reason about the request
    return AwaitApproval(                            # park — nothing irreversible yet
        on_approve=Next("issue_refund", ctx.context),
        on_reject=Complete(),
        justification="Approve the $5,000 refund?",
    )

@worker.operation("refund", "issue_refund")         # step 2: runs only after a human approves
async def issue_refund(ctx):
    ...                                              # the sensitive action, now sanctioned
    return Complete()
```

Governance is applied from the control plane — three layers, from a per-run cap
to self-healing version rollback:

```python
from boundflow import (
    RuntimePolicy, AgentRule, AgentMetric, Op, SetModel,
    WorkflowRule, WorkflowMetric, SetVersion,
)

# 1. Runtime — a hard cap enforced *during* every run:
await cp.set_agent_runtime_policy(wf.id, "analyst", RuntimePolicy(max_cost_usd=0.25))

# 2. Agent lifecycle — after runs, downgrade the model if cost trends high:
await cp.set_agent_lifecycle_policy(wf.id, "analyst", [
    AgentRule(metric=AgentMetric.COST_USD, op=Op.GT, threshold=0.20, window=5,
              action=SetModel(value="claude-haiku-4-5")),
])

# 3. Workflow lifecycle — after repeated failures, roll the whole workflow back
#    to a known-good version automatically:
await cp.set_workflow_lifecycle_policy(wf.id, [
    WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=3,
                 action=SetVersion(target=1)),
])
```

Workflow rules can also `Pause` a workflow or put it on `Cooldown` instead of
rolling back. See [`sdk/python/boundflow/examples/`](sdk/python/boundflow/examples/) for runnable examples.

---

## Observability

Observability is first-class and **OpenTelemetry-native** — no proprietary format,
no lock-in, so it plugs straight into the telemetry stack you already run. Two
layers: **run traces** (execution telemetry you export to your own backend) and a
**governance audit log** (decisions, kept server-side and queryable).

**Run traces.** Every operation emits an `OperationTrace` — the `operation → agent
→ llm/tool` tree with token usage and full prompt/response content — to a pluggable
sink you own. Built-ins: `LoggingTraceSink`, `JsonlFileTraceSink`, and
`OTelTraceSink`, which maps onto OpenTelemetry GenAI semantic conventions and ships
spans over OTLP to any backend (Jaeger, Tempo, Langfuse, Phoenix, …); all operations
of one run share a `trace_id`.

```python
from boundflow import BoundFlowWorker
from boundflow.trace import OTelTraceSink

worker = BoundFlowWorker(llm=..., trace_sink=OTelTraceSink(tracer))
```

See [`sdk/python/examples/otel/`](sdk/python/examples/otel/) for a runnable
OTLP → Jaeger setup.

**Approval audit.** Approval decisions are governance, not telemetry, so the
decision / actor / timing live in a durable server-side audit log — the trace
carries only the `approval_id` (on the `await_approval` span) as the correlation
key. Look the record up by that id:

```python
records = await cp.get_approval_audit(approval_id="…")
# -> decision (approved | rejected | timed_out), actor, opened_at, decided_at
```

**Inventory.** `cp.list_workflows()` returns every workflow with its current
lifecycle / workflow state for dashboards.

---

## Configuration

Backend and SDK are configured through `BOUNDFLOW_*` environment variables (plus
`ANTHROPIC_API_KEY` for real agents). See
**[docs/deployment.md](docs/deployment.md)** for the full reference and the
TLS-termination setup.

> The default Postgres credentials in the compose files (`boundflow/boundflow`)
> are for **local development only** — set real credentials before any non-local
> deployment, and don't publish the Postgres port.

---

## Development

```bash
make build   # build the binary -> bin/boundflow
make test    # go test ./...
make proto   # regenerate gRPC stubs (Go + Python)
```

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for full setup, the proto workflow, and
running the Python SDK test suites. CI runs the Go + mock-LLM suites on every PR; a
separate live-LLM suite (real Anthropic calls) runs on demand.

---

## Hosted: BoundFlow Cloud (early access)

Don't want to run or manage the control plane yourself? **BoundFlow Cloud** is an
early-access managed deployment — same gRPC API, same `pip install boundflow` SDK.
Inference stays bring-your-own, so your Anthropic key and token spend remain yours;
we just run the control plane.

It's early and design-partner–oriented while we onboard the first users —
**[reach out](mailto:arjunvlama1@gmail.com)** if you'd like in.

---

## License

- **Backend** — [Apache-2.0](LICENSE).
- **Python SDK** (`sdk/python`) — [MIT](sdk/python/LICENSE).
