#!/bin/bash
# run.sh — starts all three boundflow processes locally for E2E testing.
# Usage: ./localrun/run.sh
# Requires: postgres running, boundflow binary built (make build)

set -e

DB_USER="${USER}"
DB_URL="postgres://${DB_USER}@localhost:5432/boundflow?sslmode=disable"
MIGRATIONS_DIR="./migrations"
BIN="./bin/boundflow"

if [ ! -f "$BIN" ]; then
  echo "Binary not found. Run 'make build' first."
  exit 1
fi

echo "==> Checking postgres..."
if ! pg_isready -q; then
  echo "Postgres is not running. Start it first (e.g. pg_ctl -D /usr/local/var/postgres start)"
  exit 1
fi

echo "==> Setting up database..."
psql -U "$DB_USER" postgres -c "DROP DATABASE IF EXISTS boundflow;" 2>/dev/null || true
psql -U "$DB_USER" postgres -c "CREATE DATABASE boundflow;"
migrate -path $MIGRATIONS_DIR -database "$DB_URL" up

export BOUNDFLOW_DATABASE_URL="$DB_URL"
export BOUNDFLOW_LOG_LEVEL="debug"
export BOUNDFLOW_DEBUG="true"
export BOUNDFLOW_NUM_PARTITIONS="10"

echo "==> Starting server (port 50051)..."
$BIN -mode=server &
SERVER_PID=$!

echo "==> Starting scheduler..."
$BIN -mode=scheduler &
SCHEDULER_PID=$!

echo "==> Starting worker (port 50052)..."
$BIN -mode=worker &
WORKER_PID=$!

echo "==> Starting test worker client..."
go run ./localrun/testworker/main.go &
CLIENT_PID=$!

cleanup() {
  echo ""
  echo "==> Shutting down..."
  kill $SERVER_PID $SCHEDULER_PID $WORKER_PID $CLIENT_PID 2>/dev/null
  wait 2>/dev/null
  echo "==> Tearing down database..."
  psql -U "$DB_USER" postgres -c "DROP DATABASE IF EXISTS boundflow;" 2>/dev/null || true
  echo "==> Done."
}
trap cleanup SIGINT SIGTERM

echo ""
echo "All processes running. Press Ctrl+C to stop."
echo ""
echo "Step 1 — Create a tenant group:"
echo "  grpcurl -plaintext -d '{\"tenant_group\":{\"name\":\"test-group\"}}' localhost:50051 boundflow.v1.RegistrationService/CreateTenantGroup"
echo ""
echo "Step 2 — Create a tenant (use tenant_group_id from above):"
echo "  grpcurl -plaintext -d '{\"tenant\":{\"tenant_group_id\":\"<group-id>\",\"name\":\"test-tenant\"}}' localhost:50051 boundflow.v1.RegistrationService/CreateTenant"
echo ""
echo "Step 3 — Create a resource (use id from above as tenant_id):"
echo "  grpcurl -plaintext -d '{\"resource_type\":\"database\",\"tenant_id\":\"<tenant-id>\",\"initial_state\":{\"sku\":\"standard\"}}' localhost:50051 boundflow.v1.ResourceLifecycleService/CreateResource"
echo ""
echo "Step 4 — Check resource state (use resource_instance_id from above):"
echo "  grpcurl -plaintext -d '{\"resource_instance_id\":\"<resource-id>\"}' localhost:50051 boundflow.v1.ResourceLifecycleService/GetResourceState"
echo ""
wait
