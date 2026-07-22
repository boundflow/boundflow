#!/usr/bin/env bash
# setup.sh — run this BEFORE recording. Creates and activates the workflow on
# v1, repeating every 5s, and arms the rollback safety net (2 failures on the
# current version rolls it back to v1). By the time you hit record, the
# workflow is already running — run_demo.sh starts with a `get` against it.
set -euo pipefail
cd "$(dirname "$0")"

TENANT_ID=$(boundflow --json tenant create lifecycle-demo | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
WF_JSON=$(boundflow --json workflow create order-remediation "$TENANT_ID" --version 1 --repeat 5)
WF_ID=$(echo "$WF_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
boundflow workflow activate "$WF_ID"
boundflow policy lifecycle set-workflow "$WF_ID" --file wf_rules_rollback.json

echo "$WF_ID" > .demo_wf_id
echo "tenant:   $TENANT_ID"
echo "workflow: $WF_ID (running, rollback safety net armed)"
echo
echo "Now start worker.py in another terminal, then run ./run_demo.sh to record."
