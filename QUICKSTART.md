# BoundFlow Quickstart

Run a governed fleet of agents locally in a few minutes: a self-hostable control
plane (backend) plus a Python SDK you build against.

## Prerequisites

- **Docker** (with Compose) — runs the backend + Postgres
- **Python 3.10+** — for the SDK
- **An Anthropic API key** — your agents run on Claude (inference is yours; the
  backend never sees your key). `export ANTHROPIC_API_KEY=...`

## 1. Start the backend

```bash
docker compose -f docker-compose.dist.yml up -d
```

This pulls the backend image and starts Postgres, the server (`:50051`), the
scheduler, and a worker (`:50052`). Schema migrations run automatically. To pin a
version: `BOUNDFLOW_IMAGE=ghcr.io/boundflow/boundflow:v0.1.0 docker compose ... up -d`.

## 2. Provision an API key

```bash
docker compose -f docker-compose.dist.yml run --rm server -mode=provision -name=me
```

Copy the printed `api_key` and export it:

```bash
export BOUNDFLOW_API_KEY=<your-api-key>
```

## 3. Install the SDK

```bash
pip install boundflow
```

## 4. Run your first workflow

Save as `hello.py` and run it (`python hello.py`):

```python
import asyncio
import os

from boundflow import (
    AgentDefinition, BoundFlowWorker, Complete, ControlPlaneClient,
    LifecycleState, WorkflowConfig,
)
from boundflow.anthropic_client import AnthropicLlmClient

# Endpoints default to localhost:50051 (control plane) and :50052 (worker), and
# BOUNDFLOW_API_KEY is read from the env. Pointing at a remote backend? Set
# BOUNDFLOW_SERVER_ADDRESS / BOUNDFLOW_WORKER_ADDRESS (or pass them explicitly).


async def main() -> None:
    # A worker hosts your workflow handlers and runs agents on your Anthropic key.
    worker = BoundFlowWorker(llm=AnthropicLlmClient(os.environ["ANTHROPIC_API_KEY"]))

    summarizer = AgentDefinition(
        name="summarizer",
        system_prompt="You summarize text in one short, plain sentence.",
        model="claude-haiku-4-5",
        output_schema={"summary": {"type": "string"}},
    )

    @worker.workflow("hello", version=1)
    async def hello(ctx):
        ctx.add_context("text", "BoundFlow runs fleets of agents under governance.")
        result = await ctx.run_agent(summarizer)
        print("  agent summary:", result.output["summary"])
        return Complete()

    worker_task = asyncio.create_task(worker.run())

    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant("quickstart")
        wf = await cp.create_workflow("hello", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)

        while await cp.get_workflow_lifecycle_state(wf.id) == LifecycleState.INVOKING:
            await asyncio.sleep(0.5)
        print("  final state:", await cp.get_workflow_lifecycle_state(wf.id))

    worker_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
```

Expected output (the summary is the model's, so wording will vary):

```
  agent summary: BoundFlow is a platform for running fleets of agents under governance.
  final state: LifecycleState.ACTIVE
```

That's the full loop: your worker registered a workflow, the control plane
scheduled and dispatched it, and a real agent ran under the platform's governance.

## More examples

Runnable examples ship with the package — same prerequisites (backend up,
`BOUNDFLOW_API_KEY` + `ANTHROPIC_API_KEY` set):

```bash
python -m boundflow.examples.hello           # the above, as a script
python -m boundflow.examples.approval_gate   # human-in-the-loop: pause for sign-off before a sensitive action
```

## Useful commands

```bash
# Inspect the database
docker compose -f docker-compose.dist.yml exec postgres psql -U convergeplane

# Tail logs
docker compose -f docker-compose.dist.yml logs -f server scheduler worker

# Stop (keeps data)
docker compose -f docker-compose.dist.yml down

# Stop and wipe all data
docker compose -f docker-compose.dist.yml down -v
```

---

**License:** the Python SDK is open source (MIT). The backend is free for local
evaluation and development — see [BACKEND-LICENSE.txt](BACKEND-LICENSE.txt).
