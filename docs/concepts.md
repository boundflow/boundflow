# Concepts

## Core objects

- **Workflow** — the managed entity. Belongs to a tenant, has a type + version,
  and moves through lifecycle states (`active → scheduled → invoking →
  awaiting_approval → …`).
- **Agent** — a named LLM executor inside an operation handler: a model, system
  prompt, tool callbacks, and an output schema. Metrics are collected per run.
- **Approval gate** — a workflow can pause mid-execution for a human to approve or
  reject a proposed action before continuing.
- **Runtime policy** — hard limits enforced *during* a run (max LLM calls, max
  tokens/call, per-tool limits, max cost).
- **Lifecycle policy** — rules evaluated *after* runs, on aggregated metrics:
  switch model, cool down, roll back a version, pause.

## Lifecycle states

A workflow's `lifecycle_state` is a projection of its in-flight run:

| State | Meaning |
|---|---|
| `active` | Idle — no run in flight. |
| `scheduled` | A run is queued, waiting for a worker to pick it up. |
| `blocked` | A queued run has sat unowned past the threshold — nobody picked it up. |
| `invoking` | A worker is actively executing the operation. |
| `awaiting_approval` | Parked at an approval gate for a human decision. |
| `interrupted` | A platform failure (e.g. a lost worker mid-operation) interrupted the run; the workflow is disabled until resolved. |

Customer-side failures (an uncaught exception in your handler, or an operation
timeout) are *soft* — they count against the workflow's failure metric but leave
it active. Only platform interruptions disable a workflow and require
`resolve_interrupted_workflow`.

## Architecture

The control plane runs as three process modes off one binary, sharing a Postgres
database. Your SDK worker connects over gRPC and runs the actual agents.

```
┌────────────────────┐        gRPC        ┌──────────────────────┐
│  Your client       │ ─────────────────▶ │   server  :50051      │
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
