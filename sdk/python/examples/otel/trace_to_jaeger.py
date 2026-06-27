"""Ship BoundFlow run traces to Jaeger over OpenTelemetry, then view them in the UI.

Runs a TWO-operation workflow (assess → Next → finalize). Both operations of the
one invocation share a trace_id, so they render as a SINGLE trace in Jaeger — the
`operation → agent → chat/tool` span tree, with model, token usage, and the
prompt/response content on each span.

Prereqs
-------
1. BoundFlow backend up (repo root):     docker compose up -d
2. Jaeger up (this dir):                 docker compose -f docker-compose.yml up -d
3. A key — provision one and export it:
       docker compose run --rm server -mode=provision -name=otel-demo
       export BOUNDFLOW_API_KEY=<the printed api_key>
4. The optional OTel deps:               pip install 'boundflow[otel]'
5. (optional) export ANTHROPIC_API_KEY=… to drive a real model; otherwise a mock.

Run
---
    python trace_to_jaeger.py
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import urllib.request

from opentelemetry import trace as ot
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    ControlPlaneClient,
    LifecycleState,
    MockContext,
    MockLlmClient,
    Next,
    RuntimePolicy,
    Tool,
    Turn,
    WorkflowConfig,
    submit,
    turn,
)
from boundflow.trace import OTelTraceSink

WORKER_ADDR = "http://localhost:50052"
SERVER_ADDR = "http://localhost:50051"
JAEGER_OTLP = os.environ.get("JAEGER_OTLP", "localhost:4317")
JAEGER_UI = os.environ.get("JAEGER_UI", "http://localhost:16686")
HAIKU = "claude-haiku-4-5-20251001"


def build_tracer() -> tuple[TracerProvider, object]:
    """An OTLP tracer pointed at Jaeger. Swap the exporter endpoint to send the
    exact same spans to any other OTel backend (Tempo, Langfuse, Phoenix, …)."""
    provider = TracerProvider(resource=Resource.create({"service.name": "boundflow"}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=JAEGER_OTLP, insecure=True)))
    ot.set_tracer_provider(provider)
    return provider, provider.get_tracer("boundflow")


def make_llm() -> tuple[object, str]:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        from boundflow.anthropic_client import AnthropicLlmClient
        return AnthropicLlmClient(key), HAIKU

    def script(ctx: MockContext) -> Turn:
        # The assessor calls the tool once then submits; the finalizer just submits.
        if "[assess]" in ctx.system_prompt:
            return turn(300, 120, "lookup") if ctx.turn_index == 0 else submit()
        return submit()

    return MockLlmClient(script), "mock-model"


def jaeger_trace_url(request_id: str) -> tuple[str, str]:
    """OTelTraceSink derives the OTel trace_id deterministically from the run id
    (sha256(request_id)[:16]); recompute it here to build the direct UI link."""
    trace_id = int.from_bytes(hashlib.sha256(request_id.encode()).digest()[:16], "big") or 1
    hex_id = f"{trace_id:032x}"
    return hex_id, f"{JAEGER_UI}/trace/{hex_id}"


def wait_for_trace_in_jaeger(hex_id: str, attempts: int = 20) -> int:
    """Poll Jaeger's query API until the exported spans are searchable."""
    url = f"{JAEGER_UI}/api/traces/{hex_id}"
    for _ in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                data = json.load(resp)
                spans = data.get("data") and data["data"][0].get("spans")
                if spans:
                    return len(spans)
        except Exception:
            pass
        import time
        time.sleep(0.5)
    return 0


async def main() -> None:
    api_key = os.environ.get("BOUNDFLOW_API_KEY")
    if not api_key:
        raise SystemExit(
            "Set BOUNDFLOW_API_KEY first:\n"
            "  docker compose run --rm server -mode=provision -name=otel-demo\n"
            "  export BOUNDFLOW_API_KEY=<the printed api_key>"
        )

    provider, tracer = build_tracer()
    sink = OTelTraceSink(tracer)
    llm, model = make_llm()

    worker = BoundFlowWorker(WORKER_ADDR, llm, api_key=api_key, trace_sink=sink)

    async def lookup(_args: dict) -> dict:
        return {"answer": 42}

    assessor = AgentDefinition(
        name="assessor",
        system_prompt="[assess] Call the `lookup` tool exactly once, then call submit_result with done=true.",
        model=model,
        tools=[Tool("lookup", "Look up the answer.", lookup)],
        output_schema={"done": {"type": "boolean"}},
    )
    finalizer = AgentDefinition(
        name="finalizer",
        system_prompt="[finalize] Call submit_result with done=true.",
        model=model,
        output_schema={"done": {"type": "boolean"}},
    )

    @worker.workflow("otel-demo", version=1)
    async def _entry(ctx):
        await ctx.run_agent(assessor)
        return Next("finalize", ctx.context, timeout=60)

    @worker.operation("otel-demo", "finalize")
    async def _finalize(ctx):
        await ctx.run_agent(finalizer)
        return Complete()

    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.2)

    async with ControlPlaneClient(SERVER_ADDR, api_key=api_key) as cp:
        tenant = await cp.create_tenant("otel-demo")
        wf = await cp.create_workflow("otel-demo", tenant.id, WorkflowConfig(version=1))
        await cp.set_agent_runtime_policy(wf.id, "assessor", RuntimePolicy(max_llm_calls=8))
        await cp.set_agent_runtime_policy(wf.id, "finalizer", RuntimePolicy(max_llm_calls=8))
        await cp.activate_workflow(wf.id)

        print(f"invoking otel-demo ({model}) …")
        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)

        # Wait until the run leaves INVOKING (entry → finalize → complete).
        while await cp.get_workflow_lifecycle_state(wf.id) == LifecycleState.INVOKING:
            await asyncio.sleep(0.5)

        await cp.delete_workflow(wf.id)

    # Flush spans to Jaeger before exit.
    worker_task.cancel()
    await asyncio.gather(worker_task, return_exceptions=True)
    provider.force_flush()
    provider.shutdown()

    hex_id, url = jaeger_trace_url(request_id)
    print(f"run id : {request_id}")
    print(f"trace  : {hex_id}")
    n = wait_for_trace_in_jaeger(hex_id)
    if n:
        print(f"✔ {n} spans landed in Jaeger (2 operations + agent runs + llm/tool, one trace).")
    else:
        print("… spans not visible yet; give Jaeger a few seconds and refresh.")
    print(f"open   : {url}")


if __name__ == "__main__":
    asyncio.run(main())
