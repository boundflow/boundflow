#!/usr/bin/env bash
# quickstart.sh — full boundflow walkthrough from a fresh stack
#
# Prerequisites:
#   docker compose up -d --build --wait
#   export BOUNDFLOW_API_KEY=$(docker compose run --rm server -mode=provision -name=me | grep api_key | awk '{print $3}')
#   export BOUNDFLOW_SERVER_ADDRESS=http://localhost:50051
#   pip install boundflow
#
# On PowerShell (Windows):
#   $env:BOUNDFLOW_API_KEY    = "your-key"
#   $env:BOUNDFLOW_SERVER_ADDRESS = "http://localhost:50051"
set -euo pipefail

echo "=== boundflow quickstart ==="
echo

# ── 1. Explore the CLI ───────────────────────────────────────────────────────
echo "-- help --"
boundflow --help
echo

# ── 2. Create a tenant ───────────────────────────────────────────────────────
echo "-- create tenant --"
boundflow tenant create acme

TENANT_ID=$(boundflow --json tenant create acme-ops | python -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "tenant id: $TENANT_ID"
echo

# ── 3. Create + activate a workflow ──────────────────────────────────────────
echo "-- create workflow --"
boundflow workflow create ticket-triage "$TENANT_ID" --version 1

WORKFLOW_ID=$(boundflow --json workflow create order-remediation "$TENANT_ID" --version 1 | python -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "workflow id: $WORKFLOW_ID"

echo
echo "-- activate --"
boundflow workflow activate "$WORKFLOW_ID"

# ── 4. List and inspect workflows ────────────────────────────────────────────
echo
echo "-- list (table) --"
boundflow workflow list

echo
echo "-- get workflow state --"
boundflow workflow get "$WORKFLOW_ID"

# ── 5. Invoke (dispatches to a worker if one is connected) ───────────────────
echo
echo "-- invoke --"
boundflow workflow invoke "$WORKFLOW_ID"

# ── 6. Set model pricing ─────────────────────────────────────────────────────
echo
echo "-- pricing --"
boundflow pricing set claude-sonnet-4-6 --input 3.00 --output 15.00
boundflow pricing list

# ── 7. View audit log (empty on a fresh stack without worker runs) ────────────
echo
echo "-- audit log --"
boundflow audit log "$WORKFLOW_ID"

# ── 8. Clean up ──────────────────────────────────────────────────────────────
echo
echo "-- delete workflow --"
boundflow workflow delete "$WORKFLOW_ID" --yes

echo
echo "=== done ==="
