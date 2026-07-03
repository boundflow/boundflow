# Deployment

## Self-hosting

The backend ships as a single container image run in different modes, backed by
one Postgres database. The distribution compose file
[`docker-compose.dist.yml`](https://github.com/boundflow/boundflow/blob/boundflow/docker-compose.dist.yml)
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

!!! danger "Change the default credentials"
    The default Postgres credentials in the compose files (`boundflow/boundflow`)
    are for **local development only**. Set real credentials before any non-local
    deployment, and don't publish the Postgres port.

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

!!! note "Custom CA / self-signed certs"
    The SDK currently validates against **system root CAs** only. End-to-end TLS to
    a private-CA or self-signed certificate (e.g. on localhost) is not yet
    configurable from the SDK — front the server with a publicly-trusted cert for
    now.

A minimal Caddy terminator, for reference:

```
boundflow.example.com {
    reverse_proxy h2c://server:50051
}
```
