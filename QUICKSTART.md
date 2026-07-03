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

A workflow is a Python function you register on a worker; inside it, you run
agents. The essence of one:

```python
@worker.workflow("hello", version=1)
async def hello(ctx):
    ctx.add_context("text", "BoundFlow runs fleets of agents under governance.")
    result = await ctx.run_agent(summarizer)   # a real Claude call, on your key
    print("summary:", result.output["summary"])
    return Complete()
```

The full runnable version — worker setup, invoke, and waiting on the result — ships
with the package. Run it:

```bash
python -m boundflow.examples.hello
```

You'll see the agent's summary print, then `done: completed` — that's the full loop:
your worker registered a workflow, the control plane scheduled and dispatched it, and
a real agent ran under the platform's governance. Full source:
[`boundflow/examples/hello.py`](sdk/python/boundflow/examples/hello.py).

## More examples

Runnable examples ship with the package (backend up + `BOUNDFLOW_API_KEY` set):

```bash
# Real agents — also need ANTHROPIC_API_KEY:
python -m boundflow.examples.hello            # the above, as a script
python -m boundflow.examples.approval_gate    # human-in-the-loop: pause for sign-off before a sensitive action

# Governance policies (deterministic mock LLM — no Anthropic key needed):
python -m boundflow.examples.runtime_caps     # agent runtime: a cost cap halts a runaway agent
python -m boundflow.examples.model_switching  # agent lifecycle: auto-swap the model based on metrics
python -m boundflow.examples.self_healing     # workflow lifecycle: pause after repeated failures
```

## Useful commands

```bash
# Inspect the database
docker compose -f docker-compose.dist.yml exec postgres psql -U boundflow

# Tail logs
docker compose -f docker-compose.dist.yml logs -f server scheduler worker

# Stop (keeps data)
docker compose -f docker-compose.dist.yml down

# Stop and wipe all data
docker compose -f docker-compose.dist.yml down -v
```

---

**License:** the backend is Apache-2.0; the Python SDK is MIT. See [LICENSE](LICENSE).
