#!/bin/bash
# dev.sh — interactive dev runner for boundflow.
# Prompts for config, starts all components, resets DB on start, drops on exit.
# Usage: ./localrun/dev.sh

set -e
cd "$(dirname "$0")/.."

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${GREEN}==>${NC} $*"; }
warn()    { echo -e "${YELLOW}warn:${NC} $*"; }
die()     { echo -e "${RED}error:${NC} $*" >&2; exit 1; }
section() { echo -e "\n${CYAN}──── $* ────${NC}"; }

prompt() {
  local var="$1" label="$2" default="$3"
  read -rp "  $label [$default]: " val
  eval "$var=\"${val:-$default}\""
}

# ── Prerequisites ─────────────────────────────────────────────────────────────
section "Killing existing processes"
pkill boundflow 2>/dev/null && info "Killed existing boundflow processes." || info "No existing processes found."
sleep 1

section "Checking prerequisites"
pg_isready -q || die "Postgres is not running."
command -v migrate &>/dev/null || die "'migrate' not found. Install with: brew install golang-migrate"
[ -f "./bin/boundflow" ] || die "Binary not found. Run 'make build' first."
info "All prerequisites met."

# ── Config prompts ─────────────────────────────────────────────────────────────
section "Configuration"
prompt DB_HOST       "DB host"              "localhost"
prompt DB_PORT       "DB port"              "5432"
prompt DB_NAME       "DB name"              "boundflow"
prompt DB_USER       "DB user"              "$USER"
prompt SERVER_PORT   "Server gRPC port"     "50051"
prompt WORKER_PORT   "Worker gRPC port"     "50052"
prompt NUM_SCHEDULERS "Number of schedulers" "1"
prompt NUM_WORKERS   "Number of workers"    "1"
prompt LOG_LEVEL     "Log level"            "debug"

DB_URL="postgres://${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}?sslmode=disable"

echo ""
info "DB URL     : $DB_URL"
info "Server port: $SERVER_PORT"
info "Worker port: $WORKER_PORT"
info "Schedulers : $NUM_SCHEDULERS"
info "Workers    : $NUM_WORKERS"
info "Log level  : $LOG_LEVEL"

# ── DB reset ──────────────────────────────────────────────────────────────────
section "Resetting database"
psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" postgres \
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$DB_NAME' AND pid <> pg_backend_pid();" \
  &>/dev/null || true
psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" postgres -c "DROP DATABASE IF EXISTS $DB_NAME;" || die "Failed to drop DB."
psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" postgres -c "CREATE DATABASE $DB_NAME;" || die "Failed to create DB."
migrate -path ./migrations -database "$DB_URL" up || die "Migrations failed."
info "Database ready."

# ── Start processes ────────────────────────────────────────────────────────────
PIDS=()
LOG_DIR="/tmp/cp-dev"
mkdir -p "$LOG_DIR"

BIN="./bin/boundflow"
BASE_ENV="BOUNDFLOW_DATABASE_URL=$DB_URL BOUNDFLOW_LOG_LEVEL=$LOG_LEVEL BOUNDFLOW_DEBUG=true BOUNDFLOW_NUM_PARTITIONS=$NUM_SCHEDULERS"

start_proc() {
  local label="$1" mode="$2" log="$3"
  shift 3
  env $BASE_ENV "$@" $BIN -mode=$mode > "$log" 2>&1 &
  local pid=$!
  PIDS+=($pid)
  info "Started $label (pid=$pid) → $log"
}

section "Starting components"

start_proc "server" "server" "$LOG_DIR/server.log" \
  BOUNDFLOW_GRPC_PORT=$SERVER_PORT

for i in $(seq 1 $NUM_SCHEDULERS); do
  start_proc "scheduler-$i" "scheduler" "$LOG_DIR/scheduler-${i}.log"
done

for i in $(seq 1 $NUM_WORKERS); do
  start_proc "worker-$i" "worker" "$LOG_DIR/worker-${i}.log" \
    BOUNDFLOW_WORKER_GRPC_PORT=$WORKER_PORT
done

sleep 1

# ── Cheat sheet ────────────────────────────────────────────────────────────────
section "Ready"
echo -e "Logs:  tail -f $LOG_DIR/*.log"
echo ""
echo -e "${CYAN}Create tenant group:${NC}"
echo "  grpcurl -plaintext -d '{\"tenant_group\":{\"name\":\"test-group\"}}' localhost:$SERVER_PORT boundflow.v1.RegistrationService/CreateTenantGroup"
echo ""
echo -e "${CYAN}Create tenant (replace <group-id>):${NC}"
echo "  grpcurl -plaintext -d '{\"tenant\":{\"tenant_group_id\":\"<group-id>\",\"name\":\"test-tenant\"}}' localhost:$SERVER_PORT boundflow.v1.RegistrationService/CreateTenant"
echo ""
echo -e "${CYAN}Create resource (replace <tenant-id>):${NC}"
echo "  grpcurl -plaintext -d '{\"resource_type\":\"database\",\"tenant_id\":\"<tenant-id>\",\"initial_state\":{\"sku\":\"standard\"},\"operation_timeout_seconds\":30}' localhost:$SERVER_PORT boundflow.v1.ResourceLifecycleService/CreateResource"
echo ""
echo -e "Press ${RED}Ctrl+C${NC} to stop."

# ── Cleanup ────────────────────────────────────────────────────────────────────
cleanup() {
  section "Shutting down"
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null && info "Killed pid=$pid" || true
  done
  wait "${PIDS[@]}" 2>/dev/null || true

  section "Dropping database"
  psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" postgres \
    -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$DB_NAME' AND pid <> pg_backend_pid();" \
    &>/dev/null || true
  psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" postgres -c "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null && info "Database dropped." || warn "Could not drop database."
  info "Done."
}
trap cleanup SIGINT SIGTERM EXIT

wait
