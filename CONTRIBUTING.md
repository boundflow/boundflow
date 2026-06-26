# Contributing to BoundFlow

Thanks for your interest in contributing! This guide covers local setup, the test
suite, and the proto workflow.

## Prerequisites

| Tool | For |
|---|---|
| Go (see `go.mod` for version) | backend |
| Docker + Compose | running the backend (Postgres + services) |
| Python 3.10+ | the SDK |
| [buf](https://buf.build/docs/installation) | regenerating gRPC stubs (proto changes only) |
| [grpcio-tools](https://pypi.org/project/grpcio-tools/) | Python stubs (`pip install -e "sdk/python[dev]"`) |
| [mockgen](https://github.com/uber-go/mock) | regenerating Go test mocks |

An Anthropic API key is only needed to run the real-LLM tests; the default suite
uses a mock LLM.

## Backend (Go)

```bash
make build      # -> bin/boundflow
make test       # go test ./...  (all unit tests are mock-based; no DB required)
make lint       # buf lint + golangci-lint
```

The binary is multi-mode: `-mode=server | scheduler | worker | migrate | provision`.

## Running the full stack

```bash
docker compose up -d --build
docker compose run --rm server -mode=provision -name=dev   # prints an API key
```

`docker compose down -v` wipes the database for a clean slate.

## SDK + tests (Python)

The SDK tests are integration tests that run against a live backend (above).

```bash
cd sdk/python
pip install -e ".[dev]"

# Mock-LLM suite — no Anthropic key needed (this is what CI gates on):
BOUNDFLOW_API_KEY=<provisioned key> pytest

# Including the real-LLM tests:
BOUNDFLOW_API_KEY=<key> ANTHROPIC_API_KEY=<key> pytest
```

## Protobuf / generated code

`.proto` files live in `proto/boundflow/v1/`. The generated stubs are committed,
so you only regenerate when you change a `.proto`:

```bash
make proto          # Go (buf) + Python (grpcio-tools) — keeps both in sync
```

If you change a storage interface, regenerate the Go mocks:

```bash
make mocks
```

## Pull requests

- CI must be green: the Go suite and the mock-LLM Python suite run on every PR.
  (Real-LLM tests run nightly, not on PRs.)
- Keep changes focused; match the surrounding code's style and comment density.
- Regenerate (not hand-edit) anything under `gen/` or `boundflow/v1/`.

## License

The backend is Apache-2.0 and the Python SDK is MIT. By contributing, you agree
that your contributions are licensed under the same terms as the files you change.
