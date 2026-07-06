# Observability

Two layers: **run traces** (execution telemetry you export to your own backend)
and a **governance audit log** (decisions, kept server-side and queryable).

## Run traces

Every operation emits an `OperationTrace` — the `operation → agent → llm/tool`
tree with token usage and full prompt/response content — to a pluggable sink you
own. Built-ins: `LoggingTraceSink`, `JsonlFileTraceSink`, and `OTelTraceSink`,
which maps onto OpenTelemetry GenAI semantic conventions and ships spans over OTLP
to any backend (Jaeger, Tempo, Langfuse, Phoenix, …). All operations of one run
share a `trace_id`.

```python
from boundflow import BoundFlowWorker
from boundflow.trace import OTelTraceSink

worker = BoundFlowWorker(llm=..., trace_sink=OTelTraceSink(tracer))
```

See [`sdk/python/examples/otel/`](https://github.com/boundflow/boundflow/blob/main/sdk/python/examples/otel/)
for a runnable OTLP → Jaeger setup.

## Approval audit

Approval decisions are governance, not telemetry, so the decision / actor /
timing live in a durable server-side audit log — the trace carries only the
`approval_id` (on the `await_approval` span) as the correlation key. Look the
record up by that id:

```python
records = await cp.get_approval_audit(approval_id="…")
# -> decision (approved | rejected | timed_out), actor, opened_at, decided_at
```

## Inventory & run history

```python
# Every workflow with its current lifecycle / workflow state:
workflows = await cp.list_workflows()

# The tenants in your tenant group:
tenants = await cp.list_tenants()

# Per-workflow run history, with each run's outcome:
runs = await cp.list_workflow_runs(workflow_id)

# The status/outcome of a single run, by the request id invoke returned:
info = await cp.get_request_info(request_id)
```

Each run reports a `run_outcome` — `successful`, `customer_marked_failure`,
`uncaught_operation_exception`, `operation_timeout`, or `interrupted` — plus a
failure reason where applicable.
