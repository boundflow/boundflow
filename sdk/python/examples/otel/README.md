# Run traces → OpenTelemetry → Jaeger

`OTelTraceSink` maps each BoundFlow run onto OpenTelemetry spans
(`operation → agent → chat/tool`, with GenAI semantic-convention attributes) and
exports them over OTLP. This example points that sink at **Jaeger** — a single
container that ingests OTLP, stores it, and serves a web UI to view the trace.

The example workflow has **two operations** (`assess → Next → finalize`). Both
operations of one invocation share a `trace_id`, so they render as **one** trace
in Jaeger, not two.

## Run

```bash
# 1. BoundFlow backend (from the repo root)
docker compose up -d

# 2. Jaeger (from this directory)
docker compose -f docker-compose.yml up -d

# 3. Provision a key and export it
docker compose run --rm server -mode=provision -name=otel-demo
export BOUNDFLOW_API_KEY=<the printed api_key>

# 4. Install the optional OTel deps
pip install 'boundflow[otel]'

# 5. (optional) use a real model instead of the mock
export ANTHROPIC_API_KEY=sk-ant-...

# 6. Run it
python trace_to_jaeger.py
```

It prints the run id and a direct Jaeger link, e.g.
`http://localhost:16686/trace/<id>`. Open it to see the span tree — click any
`chat` span for the model, token usage, and the prompt/response content.

## Point it elsewhere

`OTelTraceSink` takes any OTel tracer. To send the same spans to Tempo, Langfuse,
Phoenix, or a collector, change only the exporter endpoint in `build_tracer()`
(`JAEGER_OTLP` env var) — the sink and workflow code stay identical.

## Persist traces across restarts

Jaeger here uses in-memory storage (cleared on restart). For durable storage,
switch to its embedded **Badger** DB — see the commented block in
`docker-compose.yml`.
