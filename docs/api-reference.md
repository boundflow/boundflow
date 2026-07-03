# API reference

The Python SDK exposes two entry points: `ControlPlaneClient` for
managing/observing workflows, and `BoundFlowWorker` for running them. This page is
a high-level map; see the docstrings in
[`sdk/python/boundflow`](https://github.com/boundflow/boundflow/tree/boundflow/sdk/python/boundflow)
for full signatures.

!!! warning "Pre-1.0"
    The SDK surface and the underlying gRPC protobufs may change before 1.0.

## `ControlPlaneClient`

Construct with a server address and API key (both default from the environment):

```python
async with ControlPlaneClient(api_key=...) as cp:
    ...
```

### Tenants & tenant groups

| Method | Purpose |
|---|---|
| `create_tenant(name)` | Create a tenant in the caller's tenant group. |
| `list_tenants()` | List the caller's tenants. |
| `create_tenant_group(name)` | Create a tenant group. |

### Workflows

| Method | Purpose |
|---|---|
| `create_workflow(type, tenant_id, config=…)` | Register a workflow. |
| `activate_workflow(id)` | Move a workflow to `active`. |
| `invoke_workflow(id, …)` | Trigger a run; returns a `request_id`. |
| `delete_workflow(id)` | Delete a workflow. |
| `list_workflows()` | Every workflow with its lifecycle / workflow state. |
| `get_workflow_lifecycle_state(id)` | Current lifecycle state. |
| `get_workflow_state(id)` | Current workflow (enablement) state. |

### Runs

| Method | Purpose |
|---|---|
| `list_workflow_runs(id)` | Run history with per-run outcomes. |
| `get_request_info(request_id)` | Status + outcome of a single run. |
| `resolve_interrupted_workflow(id, request_id)` | Clear an interruption and re-activate. |

### Policies & pricing

| Method | Purpose |
|---|---|
| `set_agent_runtime_policy(id, agent, policy)` | Hard per-run caps. |
| `set_agent_lifecycle_policy(id, agent, rules)` | Post-run model switching. |
| `set_workflow_lifecycle_policy(id, rules)` | Cooldown / rollback / pause. |
| `set_model_pricing(model_id, …)` / `list_model_pricing()` | Per-tenant-group pricing. |

### Approvals & audit

| Method | Purpose |
|---|---|
| `approve_workflow(id, …)` / `reject_workflow(id, …)` | Resolve an approval gate. |
| `get_approval_audit(approval_id=…)` | Look up an approval decision. |
| `get_policy_audit(…)` | Look up lifecycle policy-action firings. |

## `BoundFlowWorker`

Register workflow handlers and connect to the backend:

```python
worker = BoundFlowWorker(llm=AnthropicLlmClient(...))

@worker.workflow("triage", version=1)
async def triage(ctx):
    await ctx.run_agent(AgentDefinition(...))
    return Complete()

await worker.run()
```

Inside a handler, `ctx` provides `add_context(...)`, `run_agent(...)`,
`mark_failed()`, and follow-on operation registration (e.g. approval branches).
Attach a `trace_sink=` to export run telemetry — see
[Observability](observability.md).
