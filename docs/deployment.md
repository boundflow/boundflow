# Deployment

## Self-hosting

The backend ships as a single container image run in different modes, backed by
one Postgres database. The distribution compose file
[`docker-compose.dist.yml`](https://github.com/boundflow/boundflow/blob/main/docker-compose.dist.yml)
brings up Postgres, `server`, `scheduler`, and `worker` together.

```bash
docker compose -f docker-compose.dist.yml up -d
docker compose -f docker-compose.dist.yml run --rm server -mode=provision -name=me
```

## Configuration

Backend environment variables are all prefixed `BOUNDFLOW_`:

| Variable | Applies to | Purpose |
|---|---|---|
| `DATABASE_URL` | all | Postgres connection string. |
| `GRPC_PORT` | server | Client-facing gRPC port (default 50051). |
| `WORKER_GRPC_PORT` | worker | Worker-facing gRPC port (default 50052). |
| `NUM_PARTITIONS` | scheduler | Scheduler partition count. |
| `JOB_TIMEOUT_SECS` | scheduler | Default job timeout. |
| `LOG_LEVEL` / `DEBUG` | all | Logging. |

SDK-side: `BOUNDFLOW_API_KEY`, `BOUNDFLOW_SERVER_ADDRESS` /
`BOUNDFLOW_WORKER_ADDRESS` (default to localhost), and `ANTHROPIC_API_KEY` for
real agents.

### Secrets — the `.env` file

`docker compose` automatically reads a `.env` file next to the compose file, so
that's where deployment secrets go. Copy the template and set your values:

```bash
cp .env.example .env
# BOUNDFLOW_DB_PASSWORD is required — the stack won't start without it.
# Generate a strong one:  echo "BOUNDFLOW_DB_PASSWORD=$(openssl rand -hex 16)" >> .env
```

`.env` is gitignored; never commit real secrets. `BOUNDFLOW_DB_PASSWORD` feeds
**both** the bundled Postgres container and the backend's connection string.
`docker-compose.dist.yml` ships **no default** for it — a deployment must set its own,
so it can't accidentally run on a known password. (The dev compose,
`docker-compose.yml`, keeps a local default for tests.)

### Production database — bring your own

For anything beyond a local trial, don't rely on the bundled `postgres` container —
point the backend at a **managed Postgres** (RDS / Cloud SQL / Azure DB) over TLS.
Set `BOUNDFLOW_DATABASE_URL` in `.env`; it overrides the bundled URL entirely:

```bash
# .env
BOUNDFLOW_DATABASE_URL=postgres://user:password@your-db-host:5432/boundflow?sslmode=require
```

Then remove the bundled `postgres` service (and the `depends_on: postgres` entries)
from your compose file — or override them in a `docker-compose.override.yml` — and
run `-mode=migrate` once against your database to create the schema.

> [!WARNING]
> **Don't publish the Postgres port.** The bundled Postgres isn't published to the
> host. If you expose it, put it behind your network's controls — and set
> `BOUNDFLOW_DB_PASSWORD` to a strong secret (required; see above).

## TLS

The Go server speaks **plaintext gRPC**; TLS is expected to be **terminated at the
edge** — a reverse proxy or load balancer (Caddy, nginx, Envoy, or a cloud LB) in
front of the server that presents the certificate and forwards plaintext to the
backend. This is a standard gRPC deployment pattern.

The SDK selects TLS by URL scheme: an `https://` endpoint uses a secure channel
(validated against system root CAs); anything else is insecure. So point the SDK
at your terminating proxy over `https://` in production:

```bash
export BOUNDFLOW_SERVER_ADDRESS=https://boundflow.example.com:443
export BOUNDFLOW_WORKER_ADDRESS=https://boundflow.example.com:8443
```

> [!NOTE]
> **Custom CA / self-signed certs.** The SDK currently validates against **system
> root CAs** only. End-to-end TLS to a private-CA or self-signed certificate (e.g.
> on localhost) is not yet configurable from the SDK — front the server with a
> publicly-trusted cert for now.

A minimal Caddy terminator, for reference:

```
boundflow.example.com {
    reverse_proxy h2c://server:50051
}
```

## Continuous deployment (BoundFlow's own test cloud)

This section describes how *we* run the shared test environment. It isn't required
for self-hosting.

Every green `Tests` run on `main` builds an image, runs migrations, and rolls the
three container apps. Nothing is tagged and no version is bumped — a tag still means
a deliberate stable SDK release, and nothing else.

Deploying continuously also keeps the server ahead of every published SDK, which is
the only safe direction: a client calling an RPC its server lacks fails, while a
server ahead of its client is invisible.

### Why migrations run as a job

The database has public network access disabled and sits on a delegated subnet, so a
CI runner cannot reach it. Migrations run as a Container Apps *job* inside the same
managed environment, sharing the apps' connection-string secret. The deploy fails
closed: if the migration doesn't succeed, the apps stay on the previous image rather
than starting a binary against a schema it doesn't match.

Create the job once:

```bash
RG=boundflow_test
ENV=$(az containerapp show -n boundflow-server-test -g $RG \
        --query properties.managedEnvironmentId -o tsv)
CONN=$(az containerapp secret show -n boundflow-server-test -g $RG \
        --secret-name boundflow-app-db-conn --query value -o tsv)

az containerapp job create \
  -n boundflow-migrate-test -g $RG --environment "$ENV" \
  --trigger-type Manual --replica-timeout 600 --replica-retry-limit 0 \
  --image ghcr.io/boundflow/boundflow:latest \
  --cpu 0.5 --memory 1Gi \
  --args "-mode=migrate" \
  --secrets "boundflow-app-db-conn=$CONN" \
  --env-vars "BOUNDFLOW_DATABASE_URL=secretref:boundflow-app-db-conn" \
             "BOUNDFLOW_NUM_PARTITIONS=10"
```

`BOUNDFLOW_NUM_PARTITIONS` is required even in migrate mode, and must match what the
scheduler runs with.

### Azure credentials for CI

The workflow authenticates with a federated credential — no secret is stored.

```bash
APP_ID=$(az ad app create --display-name boundflow-ci --query appId -o tsv)
az ad sp create --id "$APP_ID"
az role assignment create --assignee "$APP_ID" --role Contributor \
  --scope /subscriptions/<subscription-id>/resourceGroups/boundflow_test

az ad app federated-credential create --id "$APP_ID" --parameters '{
  "name": "main",
  "issuer": "https://token.actions.githubusercontent.com",
  "subject": "repo:boundflow/boundflow:environment:test-cloud",
  "audiences": ["api://AzureADTokenExchange"]
}'
```

Then set `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_SUBSCRIPTION_ID` as
repository *variables*, and create a `test-cloud` environment (the `subject` above
must match it).

### The SDK tracks main too

Every green `main` also publishes a PEP 440 dev release, numbered against the *next*
minor — `0.7.0.dev12` while `0.6.0` is the stable release. Dev releases sort before
the version they name and after the previous stable one, so:

```bash
pip install boundflow          # newest stable — unaffected by merges
pip install --pre boundflow    # whatever main is running
```

Nothing pushes an update to anyone's machine, so this makes main *available* rather
than applied. The console is part of the SDK and runs on the operator's machine, so
someone on the stable release sees the console from that release, not from main.
