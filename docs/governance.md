# Governance

BoundFlow applies guardrails in three layers — from a hard per-run cap to
self-healing version rollback. Runtime limits are enforced *during* a run
(SDK-side); lifecycle policies are evaluated *after* runs on aggregated metrics
(server-side).

## 1. Runtime policy — hard caps during a run

```python
from boundflow import RuntimePolicy

# Halt the agent when a single run's cost crosses the budget:
await cp.set_agent_runtime_policy(wf.id, "analyst", RuntimePolicy(max_cost_usd=0.25))
```

Runtime policies also cover `max_llm_calls`, `max_tokens_per_call`, and per-tool
call limits — the knobs that stop an output blowup or a runaway loop mid-run.

## 2. Agent lifecycle — adapt the model after runs

```python
from boundflow import AgentRule, AgentMetric, Op, SetModel

# After runs, downgrade the model if cost trends high:
await cp.set_agent_lifecycle_policy(wf.id, "analyst", [
    AgentRule(metric=AgentMetric.COST_USD, op=Op.GT, threshold=0.20, window=5,
              action=SetModel(value="claude-haiku-4-5")),
])
```

## 3. Workflow lifecycle — self-heal the whole workflow

```python
from boundflow import WorkflowRule, WorkflowMetric, SetVersion

# After repeated failures, roll back to a known-good version automatically:
await cp.set_workflow_lifecycle_policy(wf.id, [
    WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=3,
                 action=SetVersion(target=1)),
])
```

Workflow rules can also `Pause` a workflow or put it on `Cooldown` instead of
rolling back.

## Approval gates

A workflow can pause mid-execution for a human to approve or reject a proposed
action before continuing. While parked, the workflow reports
`awaiting_approval`; the decision is recorded in the governance audit log (see
[Observability](observability.md#approval-audit)).

See [`sdk/python/boundflow/examples/approval_gate.py`](https://github.com/boundflow/boundflow/blob/main/sdk/python/boundflow/examples/approval_gate.py)
for a runnable example.

## Metrics

Every run collects per-invocation metrics — cost, tokens, LLM calls, and per-tool
counts/failures — computed from real token usage with cache-aware, per-tenant
pricing. These are what the lifecycle policies above evaluate against.
