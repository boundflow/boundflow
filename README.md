# Boundflow

Boundflow is a platform for orchestrating long-running, stateful workflows powered by LLM agents. It provides scheduling, execution, and governance for agentic operations at scale — with built-in policy enforcement, approval gates, and automatic workflow lifecycle management.

The **Boundflow Python SDK** (`sdk/python`) lets you define workflow logic and agents using a clean async API that connects to the control plane over gRPC.

---

## Architecture

The Boundflow control plane runs as three independent process modes:

```
┌────────────────────┐        gRPC        ┌──────────────────────┐
│  Boundflow Client  │ ─────────────────▶│       Server         │
│  (SDK /            │                    │  :50051              │
│   gRPC direct)     │                    │  RegistrationService │
│                    │                    │  LifecycleService    │
└────────────────────┘                    └──────────────────────┘
                                                     │
                                               PostgreSQL (shared)
                                                     │
                                          ┌──────────▼───────────┐
                                          │      Scheduler        │
                                          │  (N partition workers)│
                                          │  Polls due requests,  │
                                          │  writes jobs to DB    │
                                          └──────────────────────┘
                                                     │
                                               PostgreSQL (shared)
                                                     │
┌────────────────────┐                    ┌──────────▼───────────┐
│   Python Worker    │◀───────────────────│       Worker         │
│  (BoundFlowWorker) │    gRPC stream     │  :50052              │
│  Runs LLM agents   │                    │  Polls DB for jobs,  │
│  & tool callbacks  │                    │  streams ops to SDK  │
└────────────────────┘                    └──────────────────────┘
```

| Mode | Responsibility |
|------|----------------|
| `server` | Accepts gRPC calls from clients. Manages resource/workflow lifecycle, approval flow, and policy configuration. |
| `scheduler` | Partition-based distributed scheduler. Polls due customer requests and writes jobs to the database. Runs lifecycle policy evaluation (cooldown, version rollback). |
| `worker` | Polls the database for pending jobs and dispatches them to SDK workers over a bidirectional gRPC stream. |

All three modes share a single PostgreSQL database.

---

## Core Concepts

**ResourceInstance / Workflow** — The central entity being managed. A workflow belongs to a tenant, has a resource type and version, and transitions through lifecycle states (`creating → active → reconciling → awaiting_approval → deleted`). Its workflow state (`active`, `paused`, `cooldown`, `disabled`) controls whether new invocations are dispatched.

**Job** — A single execution unit. Created by the scheduler when a CustomerRequest is due, it tracks the operation being run and its status through `pending → running → awaiting_approval → completed/failed`.

**Agent** — A named LLM executor within an operation handler. Each agent has a model, system prompt, set of allowed tool callbacks, and an output schema. Per-invocation metrics (cost, LLM calls, token counts, tool call counts/failures) are collected and evaluated against policies.

**Approval Gate** — A workflow can pause mid-execution and ask a human to approve or reject a proposed action before continuing. If approved, a specified follow-on operation runs; if rejected, an alternate path (or completion) is taken.

**WorkflowLifecyclePolicy** — Server-side rules that automatically transition a workflow based on aggregated metrics. Supported actions: `pause`, `cooldown` (with configurable duration), and `set_version` (rollback or upgrade).

**AgentLifecyclePolicy** — SDK-side rules evaluated after each invocation. Supported mutations: switch the agent's model, adjust token limits, etc. — based on per-agent metrics like cost, LLM call count, or calls-per-tool.

**AgentRuntimePolicy** — SDK-side hard limits enforced during execution: maximum LLM calls per invocation, per-tool call limits, etc.

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | 1.25+ |
| PostgreSQL | 14+ |
| [golang-migrate](https://github.com/golang-migrate/migrate) | latest |
| [Buf CLI](https://buf.build/docs/installation) | latest (only for proto changes) |
| [mockgen](https://github.com/uber-go/mock) | latest (only for test mock regen) |
| [golangci-lint](https://golangci-lint.run/) | latest (only for linting) |
| Python | 3.11+ (for SDK / demo) |
| `ANTHROPIC_API_KEY` | Required only for real LLM calls |

Python dependencies can be installed via pip (`sdk/python/requirements.txt`) or conda (`sdk/python/environment.yml`).

---

## Quick Start

### Option A: Docker Compose (recommended)

The fastest path — no Go or Postgres installation required.

```bash
docker compose up --build
```

This starts five services in the correct order: Postgres → migrations → server (`:50051`) + scheduler + worker (`:50052`). The image is built automatically from the repo.

#### Provision an API key

All gRPC calls require an API key. After the stack is up, run the provisioning script once to create a tenant group and print a raw API key:

```bash
go run ./scripts/provision_customer -name "my-org" -db "postgres://boundflow:boundflow@localhost:5432/boundflow?sslmode=disable"
```

The script prints the API key once — save it. Set it in your environment for the SDK and demos:

```bash
export BOUNDFLOW_API_KEY=<key printed above>
```

To wipe all state and start fresh:

```bash
docker compose down -v
docker compose up --build
```

The Boundflow SDK demo runs on the host against the published ports:

```bash
cd sdk/python
pip install -r requirements.txt
python examples/demo.py

# or run unattended (no keypresses required):
BOUNDFLOW_DEMO_AUTO=1 python examples/demo.py
```

---

### Option B: Run from source

#### 1. Clone and build

```bash
git clone https://github.com/boundflow/boundflow.git
cd boundflow
make build
# binary: bin/boundflow
```

#### 2. Set up the database

```bash
# Create the database (assumes a local Postgres instance)
make db-create

# Run all migrations
make db-migrate
```

To use a non-default database URL:

```bash
DB_URL=postgres://user:pass@host:5432/boundflow?sslmode=disable make db-migrate
```

#### 3. Start the three components

Open three terminals (or run them as background processes):

```bash
# Terminal 1 — server (gRPC API on :50051)
./bin/boundflow -mode=server

# Terminal 2 — scheduler
./bin/boundflow -mode=scheduler

# Terminal 3 — worker (receives operations on :50052)
./bin/boundflow -mode=worker
```

All three connect to PostgreSQL using `BOUNDFLOW_DATABASE_URL` (defaults to `postgres://localhost:5432/boundflow?sslmode=disable`).

#### 4. Provision an API key

The provisioning script requires a direct Postgres connection. `dev.sh` prompts for a DB user and defaults to `$USER`, so pass whichever user you specified when running the script:

```bash
go run ./scripts/provision_customer -name "my-org" -db "postgres://$USER@localhost:5432/boundflow?sslmode=disable"
```

The script prints a raw API key once — save it and export it before running the SDK or demos:

```bash
export BOUNDFLOW_API_KEY=<key printed above>
```

---

## Configuration

All configuration is via environment variables. Every variable is optional and falls back to the shown default.

| Variable | Default | Modes | Description |
|----------|---------|-------|-------------|
| `BOUNDFLOW_DATABASE_URL` | `postgres://localhost:5432/boundflow?sslmode=disable` | all | PostgreSQL connection string |
| `BOUNDFLOW_LOG_LEVEL` | `info` | all | Log level: `debug`, `info`, `warn`, `error` |
| `BOUNDFLOW_DEBUG` | `false` | all | Enable gRPC reflection on the server |
| `BOUNDFLOW_NUM_PARTITIONS` | — | server, scheduler | Number of scheduler partitions for horizontal scaling |
| `BOUNDFLOW_GRPC_PORT` | `50051` | server | gRPC listen port for the server |
| `BOUNDFLOW_WORKER_GRPC_PORT` | `50052` | worker | gRPC listen port for the worker |
| `BOUNDFLOW_NUM_WORKERS` | `1` | worker | Number of concurrent worker goroutines |
| `BOUNDFLOW_JOB_TIMEOUT_SECS` | `300` | worker | Per-job execution timeout in seconds |

---

## Running the Demo

The Boundflow demo (`sdk/python/examples/demo.py`) walks through seven governance scenarios using a scripted mock LLM — no `ANTHROPIC_API_KEY` needed.

```bash
cd sdk/python
pip install -r requirements.txt   # or: conda env create -f environment.yml && conda activate boundflow
python examples/demo.py
```

Set `BOUNDFLOW_DEMO_AUTO=1` to run unattended (auto-advance, auto-approve, auto-reject the pause step):

```bash
BOUNDFLOW_DEMO_AUTO=1 python examples/demo.py
```

The demo covers:
1. Register three workflows: `ticket-triage`, `order-remediation`, `incident-diagnosis`
2. Trigger normal runs and exercise approval gates
3. Loop detection — agent lifecycle policy escalates model to Opus
4. Cost spike — agent lifecycle policy downgrades model to Haiku
5. `MaxLlmCalls` and per-tool call limits (runtime policies)
6. Repeated failures trigger workflow cooldown
7. Tool failure rate on a new v2 triggers automatic version rollback to v1

---

## Testing

### Go unit tests

```bash
make test
# or
go test ./...
```

### Regenerate mocks (after changing repository interfaces)

```bash
make mocks
```

### Python integration tests

Tests run against a Boundflow backend — either a local stack or a remote one (e.g. Azure Container Apps). The target addresses are set in `sdk/python/tests/conftest.py`.

`BOUNDFLOW_API_KEY` is required for all tests. `ANTHROPIC_API_KEY` is additionally required for real-LLM scenarios; mock-LLM tests run without it.

```bash
cd sdk/python
pip install -r requirements.txt

export BOUNDFLOW_API_KEY=<your key>
pytest

# with debug logging from the SDK:
pytest -s --log-cli-level=DEBUG --log-cli-format="%(name)s %(message)s"

# real-LLM tests only:
ANTHROPIC_API_KEY=sk-ant-... pytest
```

Available test scenarios:

| File | Scenario |
|------|----------|
| `tests/test_mock_llm.py` | Basic mock LLM execution |
| `tests/test_approval_gate.py` | Approval gate (approve / reject paths) |
| `tests/test_cooldown_and_resume.py` | Cooldown and resume |
| `tests/test_agent_lifecycle_policy.py` | Agent lifecycle policy evaluation |
| `tests/test_workflow_lifecycle_policy.py` | Workflow lifecycle policy (pause / version change) |
| `tests/test_tool_call_limits.py` | Per-tool call limits |
| `tests/test_periodic_workflow.py` | Periodic (recurring) workflows |

---

## Development

### Regenerate protobuf code

After editing `.proto` files under `proto/`:

```bash
make proto
# Generates Go code into gen/ and Python stubs into sdk/python/boundflow/v1/
```

Requires [Buf CLI](https://buf.build/docs/installation).

### Lint

```bash
make lint
# Runs: buf lint (proto) + golangci-lint (Go)
```

### Database management

```bash
make db-migrate          # Apply all pending migrations
make db-reset            # Drop all tables and re-apply (destructive)
make db-start            # Start local Postgres via pg_ctl
make db-stop             # Stop local Postgres via pg_ctl
```

---

## Database Schema

Seven tables underpin the system:

| Table | Purpose |
|-------|---------|
| `tenant_groups` | Organization-level containers |
| `tenants` | Individual tenants belonging to a group |
| `resource_instances` | Workflow instances with lifecycle/workflow state, policies, and metrics |
| `customer_requests` | Pending or scheduled invocations |
| `jobs` | In-flight operation execution tracking |
| `scheduler_partitions` | Distributed scheduler lease ownership |
| `agent_state` | Per-agent runtime & lifecycle policies + metrics history |
| `workflow_version_metrics` | Aggregated per-version metrics used by lifecycle policy evaluation |

---

## Project Structure

```
boundflow/
├── cmd/boundflow/main.go       # Entry point — -mode flag selects server/scheduler/worker
├── proto/boundflow/v1/         # Protocol buffer definitions
├── gen/                            # Generated protobuf code (Go)
├── internal/
│   ├── config/                     # Environment variable config loading
│   ├── domain/                     # Domain types (ResourceInstance, Job, Policy, Agent, …)
│   ├── storage/                    # Repository interfaces + PostgreSQL implementations
│   ├── service/                    # Business logic (lifecycle, registration)
│   ├── scheduler/                  # Partition scheduler, lifecycle policy engine
│   ├── server/                     # gRPC server + handler implementations
│   ├── rpcworker/                  # Worker session streaming handler
│   ├── llm/                        # Anthropic SDK client + agent orchestrator
│   ├── metrics/                    # Metrics aggregation and persistence
│   └── convert/                    # Proto ↔ domain type conversions
├── migrations/                     # SQL migration files
├── sdk/python/
│   ├── boundflow/                  # Boundflow Python SDK (BoundFlowWorker, ControlPlaneClient, policies, …)
│   ├── boundflow/v1/           # Generated protobuf stubs for Python
│   ├── examples/demo.py            # End-to-end demo application
│   ├── tests/                      # Integration tests
│   ├── requirements.txt            # pip install path
│   └── environment.yml             # conda install path
├── Makefile
├── buf.yaml / buf.gen.yaml         # Buf protobuf configuration
└── go.mod
```

---

## License

MIT — see [LICENSE](LICENSE).
