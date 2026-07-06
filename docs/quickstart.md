# Quickstart

Get a governed agent running in a few minutes. This mirrors
[`QUICKSTART.md`](https://github.com/boundflow/boundflow/blob/main/QUICKSTART.md)
in the repo.

## Prerequisites

- **Docker** (to run the backend stack).
- **Python 3.11+** (for the SDK).
- **An Anthropic API key** — your agents run on Claude; inference is yours and the
  backend never sees the key. `export ANTHROPIC_API_KEY=...`

## 1. Start the backend

Set a database password (there's no default — the stack won't start without one),
then bring it up. `docker compose` reads `.env` automatically:

```bash
echo "BOUNDFLOW_DB_PASSWORD=$(openssl rand -hex 16)" > .env
docker compose -f docker-compose.dist.yml up -d
```

This brings up Postgres plus the three process modes off one binary — `server`,
`scheduler`, and `worker` (see [Concepts](concepts.md#architecture)).

## 2. Provision an API key

The `provision` mode mints a tenant group and an API key:

```bash
docker compose -f docker-compose.dist.yml run --rm server -mode=provision -name=me
export BOUNDFLOW_API_KEY=<printed key>
```

## 3. Install the SDK

```bash
pip install boundflow
export ANTHROPIC_API_KEY=<your key>
```

## 4. Run your first workflow

```bash
python -m boundflow.examples.hello
```

Then explore the bundled examples:

```bash
python -m boundflow.examples.approval_gate   # human-in-the-loop sign-off
```

## What just happened

Your worker registered a workflow handler and connected to the backend over gRPC.
When you invoked the workflow, the scheduler wrote a job, the `worker` mode
dispatched it to your connected SDK worker, and your agent ran — under whatever
runtime and lifecycle policies you attached. See [Governance](governance.md) to
add cost caps, model switching, and approval gates.

## Operate it from the CLI

Installing the SDK also installs the **`boundflow` CLI** — the ergonomic way to
manage and observe the control plane from a shell (everything the SDK does
programmatically, you can do ad-hoc). It reads `BOUNDFLOW_API_KEY` from the env.

The run above left a workflow and a completed run behind — inspect them:

```bash
boundflow workflow list                   # your workflows and their state
boundflow workflow runs <workflow-id>     # every run, with its outcome
boundflow workflow request <request-id>   # status + outcome of one run
boundflow tenant list                     # tenants in your group
```

Add `--json` to any command for machine-readable output. See `boundflow --help`
for the full surface — workflow create/activate/invoke, approve/reject, policies,
pricing, and audit logs.
