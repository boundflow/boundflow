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

## The operator console

The same inventory in a browser, plus the actions that need a person:

```bash
pip install "boundflow[ui]"
export BOUNDFLOW_API_KEY=<your key>
boundflow ui                    # serves http://127.0.0.1:8787
```

It opens on the fleet and on everything waiting on a human — approval gates with
their justification, input gates with their prompt — each with `actor` and `reason`
fields next to the decision, recorded on the audit event exactly as `--actor` and
`--reason` are from the CLI. Per workflow you also get its runs, its version metrics,
and suspend/resume.

The console is a client of the same control plane the CLI uses; it has no identity of
its own. Your API key stays in the `boundflow ui` process and is never rendered into
the page, and the server binds `127.0.0.1` only — anyone who can reach the console can
act with your key, so to reach a remote control plane point `--server` at it rather
than exposing the console:

```bash
boundflow --server https://boundflow.example.com:443 ui
```

Creating workflows, editing config and setting policy stay in the CLI, where a policy
is JSON you can review and commit rather than a form.

### Renaming the console's words

A product built on BoundFlow can call things whatever its own users call them:

```python
from boundflow.ui import Labels, serve

serve(server, api_key, labels=Labels(
    brand="Acme", tagline="agent control",
    workflow="agent", workflows="agents",
    lifecycle="runtime state", fleet="Agents", inbox="Needs you",
))
```

`Labels` covers the console's own wording — the brand, the section headings, the
column titles. It deliberately cannot rename the values the control plane returns:
`awaiting_approval`, `cooldown`, `interrupted`, or a workflow type. Those same
strings appear in the CLI, the API and the audit log, so a console-only rename gives
an operator a word that exists nowhere else — they report an agent stuck in "Needs
sign-off" and nobody can find it in any query or log.

`boundflow.ui` also exports `render`, `views`, `Console` and `build_app`, so a
console that needs more than different words can keep the rendering and mount its
own routes.
