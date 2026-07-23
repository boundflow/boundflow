#!/usr/bin/env bash
# json_scripting.sh — using boundflow --json for automation and scripting
#
# The --json flag makes every boundflow command emit machine-readable JSON.
# Combine it with python -c, jq, or any JSON tool for chaining and filtering.
#
# Prerequisites: BOUNDFLOW_API_KEY and BOUNDFLOW_SERVER_ADDRESS must be set.
set -euo pipefail

py() { python -c "import sys,json; $1"; }

echo "=== boundflow --json scripting demo ==="
echo

# ── 1. Extract an ID from a create command ───────────────────────────────────
echo "-- extract tenant ID --"
TENANT_ID=$(boundflow --json tenant create scripting-demo | py "print(json.load(sys.stdin)['id'])")
echo "TENANT_ID=$TENANT_ID"

# ── 2. Chain: create tenant → create workflow → activate ─────────────────────
echo
echo "-- create + activate pipeline --"
WF_JSON=$(boundflow --json workflow create data-pipeline "$TENANT_ID" --version 2)
WF_ID=$(echo "$WF_JSON" | py "print(json.load(sys.stdin)['id'])")
WF_VER=$(echo "$WF_JSON" | py "print(json.load(sys.stdin)['config']['version'])")
echo "workflow_id=$WF_ID  version=$WF_VER"
boundflow workflow activate "$WF_ID"

# ── 3. Filter list output ────────────────────────────────────────────────────
echo
echo "-- filter: only active workflows owned by this tenant --"
boundflow --json workflow list | py "
rows = json.load(sys.stdin)
active = [w for w in rows if w['tenant_id'] == '$TENANT_ID' and w['workflow_state'] == 'active']
print(json.dumps(active, indent=2))
"

# ── 4. Invoke and capture request_id ────────────────────────────────────────
echo
echo "-- invoke and capture request_id --"
REQUEST_ID=$(boundflow --json workflow invoke "$WF_ID" | py "print(json.load(sys.stdin)['request_id'])")
echo "REQUEST_ID=$REQUEST_ID"

# ── 5. Check state programmatically ─────────────────────────────────────────
echo
echo "-- check lifecycle state in a script --"
STATE=$(boundflow --json workflow get "$WF_ID" | py "print(json.load(sys.stdin)['lifecycle_state'])")
echo "lifecycle_state=$STATE"

if [ "$STATE" = "invoking" ]; then
    echo "  workflow is running — a worker will pick it up"
fi

# ── 6. Batch: list all workflows and pretty-print versions ───────────────────
echo
echo "-- batch: summarise all workflows --"
boundflow --json workflow list | py "
rows = json.load(sys.stdin)
for w in rows:
    print(f\"  {w['workflow_type']:30s}  v{w['version']}  {w['lifecycle_state']:12s}  {w['workflow_state']}\")
"

# ── 7. Pricing as a dict ──────────────────────────────────────────────────────
echo
echo "-- pricing: extract haiku rate --"
HAIKU_IN=$(boundflow --json pricing list | py "
rows = json.load(sys.stdin)
haiku = next(r for r in rows if r['model'] == 'claude-haiku-4-5')
print(haiku['input_per_1m_usd'])
")
echo "haiku input rate: \$$HAIKU_IN / 1M tokens"

# ── 8. jq alternative (if jq is installed) ───────────────────────────────────
if command -v jq &> /dev/null; then
    echo
    echo "-- same thing with jq --"
    boundflow --json workflow list | jq '[.[] | {id, type: .workflow_type, version, state: .workflow_state}]'
fi

# ── Clean up ──────────────────────────────────────────────────────────────────
echo
boundflow workflow delete "$WF_ID" --yes
echo "=== done ==="
