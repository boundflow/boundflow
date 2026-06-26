# BoundFlow

**A control plane for fleets of LLM agents — with governance built in.**

BoundFlow runs long-running, stateful agent workflows and enforces the guardrails
production agents need: per-run **cost caps**, automatic **model switching** on
cost/loop policies, **human approval gates** before sensitive actions, tool-call
limits, retries, cooldowns, and versioned rollbacks. You write agents and
workflows against a clean async SDK; the control plane schedules, dispatches, and
governs them.

Inference is **bring-your-own** — your agents call Claude with your own Anthropic
key, running in your worker. The backend never sees it and never pays for tokens.

- **Backend** — open source (Apache-2.0), self-hostable as a container.
- **Python SDK** — open source (MIT), `pip install boundflow`.

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

Agents are easy to demo and hard to operate. The moment they run unattended,
you need answers to: *What if it loops? What if it spends $50? What if it's about
to do something irreversible? Which model should it use, and when should that
change?* BoundFlow makes those **policies** instead of code:

| Concern | BoundFlow gives you |
|---|---|
| Runaway spend | `max_cost_usd` runtime cap — halts the agent when a run's cost crosses a budget |
| Wrong model for the job | Lifecycle policies that switch the model on cost/loop signals (e.g. downgrade to Haiku after a cost spike) |
| Irreversible actions | Approval gates — the workflow parks for a human decision before continuing |
| Output blowups | `max_tokens_per_call`, per-tool call limits |
| Flaky/failing runs | Cooldowns, automatic version rollback |
| Cost accounting | Per-tenant model pricing, cache-aware cost from real token usage |

Policies are evaluated server-side (lifecycle) and enforced SDK-side (runtime),
with per-invocation metrics — cost, tokens, LLM calls, per-tool counts/failures —
collected on every run.

---

## Architecture

The control plane runs as three process modes off one binary, sharing a Postgres
database. Your SDK worker connects over gRPC and runs the actual agents.

```
┌────────────────────┐        gRPC        ┌──────────────────────┐
│  Your client       │ ─────────────────▶ │   server  :50051     │
│  (ControlPlane-    │                    │   workflow lifecycle, │
│   Client)          │                    │   approvals, pricing  │
└────────────────────┘                    └──────────┬───────────┘
                                                     │  Postgres
                                          ┌──────────▼───────────┐
                                          │   scheduler           │
                                          │   polls due requests, │
                                          │   writes jobs,        │
                                          │   evaluates lifecycle │
                                          └──────────┬───────────┘
                                                     │  Postgres
┌────────────────────┐     gRPC stream    ┌──────────▼───────────┐
│  Your worker       │ ◀───────────────── │   worker  :50052      │
│  (BoundFlowWorker) │   launch / result  │   dispatches jobs to  │
│  runs agents+tools │                    │   connected SDK workers│
└────────────────────┘                    └──────────────────────┘
```

| Mode | Responsibility |
|------|----------------|
| `server` | gRPC API: workflow/tenant lifecycle, approval flow, policy + pricing configuration. |
| `scheduler` | Partition-based scheduler. Polls due requests, writes jobs, runs lifecycle policy evaluation (cooldown, version rollback). |
| `worker` | Polls for pending jobs and dispatches them to connected SDK workers over a bidirectional gRPC stream. |
| `migrate` / `provision` | One-shot modes: apply schema migrations / mint a tenant group + API key. |

---

## Core concepts

- **Workflow** — the managed entity. Belongs to a tenant, has a type + version,
  and moves through lifecycle states (`active → reconciling → awaiting_approval → …`).
- **Agent** — a named LLM executor inside an operation handler: a model, system
  prompt, tool callbacks, and an output schema. Metrics are collected per run.
- **Approval gate** — a workflow can pause mid-execution for a human to approve or
  reject a proposed action before continuing.
- **Runtime policy** — hard limits enforced *during* a run (max LLM calls, max
  tokens/call, per-tool limits, max cost).
- **Lifecycle policy** — rules evaluated *after* runs, on aggregated metrics:
  switch model, cool down, roll back a version, pause.

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

Governance is applied from the control plane — e.g. cap a run's cost:

```python
await cp.set_agent_runtime_policy(wf.id, "analyst", RuntimePolicy(max_cost_usd=0.25))
```

See [`sdk/python/boundflow/examples/`](sdk/python/boundflow/examples/) for runnable examples.

---

## Configuration

Backend (env vars, all `BOUNDFLOW_*`): `DATABASE_URL`, `GRPC_PORT` (server),
`WORKER_GRPC_PORT` (worker), `NUM_PARTITIONS`, `NUM_WORKERS`, `JOB_TIMEOUT_SECS`,
`LOG_LEVEL`, `DEBUG`.

SDK: `BOUNDFLOW_API_KEY`, `BOUNDFLOW_SERVER_ADDRESS` / `BOUNDFLOW_WORKER_ADDRESS`
(default to localhost), and `ANTHROPIC_API_KEY` for real agents.

> The default Postgres credentials in the compose files (`boundflow/boundflow`)
> are for **local development only** — set real credentials before any
> non-local deployment, and don't publish the Postgres port.

---

## Development

```bash
make build         # build the binary -> bin/boundflow
make test          # go test ./...
make proto         # regenerate gRPC stubs (Go + Python)
```

Python SDK tests run against a live backend:

```bash
docker compose up -d --build
cd sdk/python && pip install -e ".[dev]"
BOUNDFLOW_API_KEY=<provisioned key> pytest        # mock-LLM suite, no Anthropic key needed
```

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for the full setup, proto workflow, and
PR guidelines. CI runs the Go + mock-LLM suites on every PR; real-LLM tests run
nightly.

---

## License

- **Backend** — [Apache-2.0](LICENSE).
- **Python SDK** (`sdk/python`) — [MIT](sdk/python/LICENSE).
