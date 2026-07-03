# Governance audit log

BoundFlow records governance **decisions** server-side as a durable, queryable audit
log — separate from the execution telemetry traces. Three event types:

- **approval** — a human approved / rejected a gate (or it timed out): decision, actor, timing.
- **workflow policy** — a workflow-lifecycle rule fired (cooldown / pause / set_version).
- **agent policy** — an agent-lifecycle rule changed an agent's effective runtime policy (e.g. SetModel).

## Read API

```python
# unified, time-ordered (workflow_id optional — omit for the whole tenant group)
await cp.get_audit_log(workflow_id="")

# per type
await cp.get_approval_audit(workflow_id)
await cp.get_approval_audit_by_id(approval_id)        # the trace's correlation key
await cp.get_workflow_policy_audit(workflow_id)
await cp.get_agent_policy_audit(workflow_id, agent)   # agents are (workflow, name)
```

A run trace carries only the `approval_id` (the `boundflow.approval_id` span
attribute); the decision/actor/timing live here, fetched by that id.

## Run

```bash
# backend (repo root)
docker compose up -d
# provision a key
docker compose run --rm server -mode=provision -name=audit-demo
export BOUNDFLOW_API_KEY=<the printed api_key>
# run it (mock LLM — no Anthropic key needed)
python unified_audit_log.py
```

Prints the unified log: two approvals, a cooldown, a pause, and an agent `SetModel`
firing — all from one `get_audit_log()` call.
