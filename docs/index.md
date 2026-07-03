# BoundFlow

**The operational layer for the LLM agents and workflows you run unattended — cost caps, approval gates, and self-healing policy, enforced by a control plane.**

!!! warning "Preview — pre-1.0"
    BoundFlow is in **public preview**. The engine is complete and covered by Go,
    mock-LLM, and live-LLM test suites, but it has **not yet been battle-tested in
    production with external users**. APIs — including the gRPC protobufs — may
    change before 1.0. We're looking for early adopters and design partners:
    [reach out](mailto:hello@boundflow.dev).

BoundFlow runs long-running, stateful agent workflows and enforces the guardrails
you'll want *before* running agents unattended: per-run **cost caps**, automatic
**model switching** on cost/loop policies, **human approval gates** before
sensitive actions, tool-call limits, retries, cooldowns, and versioned rollbacks.
You write agents and workflows against a clean async SDK; the control plane
schedules, dispatches, and governs them.

Inference is **bring-your-own** — your agents call Claude with your own Anthropic
key, running in your worker. The backend never sees it and never pays for tokens.

- **Backend** — open source (Apache-2.0), self-hostable as a container.
- **Python SDK** — open source (MIT), `pip install boundflow`.

## Where to next

- **[Quickstart](quickstart.md)** — a governed agent running in a few minutes.
- **[Concepts](concepts.md)** — workflows, agents, approval gates, and the two
  policy layers.
- **[Governance](governance.md)** — the guardrails, with runnable examples.
- **[Observability](observability.md)** — run traces and the governance audit log.
- **[Deployment](deployment.md)** — self-hosting, configuration, and TLS.

## Why BoundFlow

Agents are easy to demo and hard to operate. The moment they run unattended, you
need answers to: *What if it loops? What if it spends $50? What if it's about to
do something irreversible? Which model should it use, and when should that
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
