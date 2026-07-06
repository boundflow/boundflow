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
