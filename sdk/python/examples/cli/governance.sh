#!/usr/bin/env bash
# governance.sh — demonstrate boundflow's policy and audit commands
#
# Shows: runtime policy (flags + file), agent and workflow lifecycle policies,
# pricing overrides, and the unified audit log.
#
# Prerequisites: BOUNDFLOW_API_KEY and BOUNDFLOW_SERVER_ADDRESS must be set.
set -euo pipefail

echo "=== boundflowgovernance demo ==="
echo

# ── Setup ────────────────────────────────────────────────────────────────────
TENANT_ID=$(boundflow --json tenant create gov-demo | python -c "import sys,json; print(json.load(sys.stdin)['id'])")
WF_ID=$(boundflow --json workflow create order-remediation "$TENANT_ID" --version 1 | python -c "import sys,json; print(json.load(sys.stdin)['id'])")
boundflowworkflow activate "$WF_ID"
echo "tenant:   $TENANT_ID"
echo "workflow: $WF_ID"
echo

# ── Runtime policy (hard caps, enforced SDK-side during the agent loop) ───────
echo "-- runtime policy via flags --"
boundflowpolicy runtime "$WF_ID" analyst \
  --max-llm-calls 5 \
  --max-cost-usd 0.50 \
  --max-tokens-per-call 4096 \
  --tool-limit rollback_fulfillment_config:1

echo
echo "-- runtime policy via JSON file --"
cat > /tmp/runtime_policy.json << 'EOF'
{
  "max_llm_calls": 3,
  "max_cost_usd": 0.10,
  "max_tokens_per_call": 2048,
  "tool_call_limits": [
    {"tool": "search", "max_calls": 5},
    {"tool": "write", "max_calls": 1}
  ]
}
EOF
boundflowpolicy runtime "$WF_ID" analyst --file /tmp/runtime_policy.json
echo

# ── Agent lifecycle policy (adapts the model based on prior-run metrics) ──────
echo "-- agent lifecycle policy --"
# If cost >= $1 last run → downgrade model to Haiku to save money.
# If tool calls to retry_job >= 3 last run → escalate to Opus for better reasoning.
cat > /tmp/agent_rules.json << 'EOF'
[
  {
    "metric": "cost_usd",
    "op": "greater_than_or_equal",
    "threshold": 1.0,
    "window": 1,
    "action": {"field": "model", "value": "claude-haiku-4-5"}
  },
  {
    "metric": "calls_per_tool",
    "op": "greater_than_or_equal",
    "threshold": 3,
    "window": 1,
    "tool": "retry_job",
    "action": {"field": "model", "value": "claude-opus-4-8"}
  }
]
EOF
boundflowpolicy lifecycle set-agent "$WF_ID" analyst --file /tmp/agent_rules.json
echo

# ── Workflow lifecycle policy (reacts to whole-workflow metrics) ──────────────
echo "-- workflow lifecycle policy --"
# After 1 failure → 10-second cooldown.
# After 3 approval rejections → pause for operator review.
cat > /tmp/wf_rules.json << 'EOF'
[
  {
    "metric": "num_failures",
    "threshold": 1,
    "action": {"kind": "cooldown", "window": 1, "seconds": 10}
  },
  {
    "metric": "approval_rejections",
    "threshold": 3,
    "action": {"kind": "pause", "window": 5}
  }
]
EOF
boundflowpolicy lifecycle set-workflow "$WF_ID" --file /tmp/wf_rules.json
echo

# ── Pricing overrides ─────────────────────────────────────────────────────────
echo "-- model pricing --"
boundflowpricing list
echo
boundflowpricing set claude-haiku-4-5 --input 1.00 --output 5.00
echo "  updated haiku pricing:"
boundflowpricing list
echo

# ── Audit (empty on a fresh workflow with no worker runs) ─────────────────────
echo "-- audit commands --"
echo "  approvals:"
boundflowaudit approvals "$WF_ID"
echo
echo "  workflow policy firings:"
boundflowaudit workflow "$WF_ID"
echo
echo "  agent policy firings:"
boundflowaudit agent "$WF_ID" analyst
echo
echo "  unified audit log:"
boundflowaudit log "$WF_ID"

# ── Clean up ──────────────────────────────────────────────────────────────────
echo
boundflowworkflow delete "$WF_ID" --yes
echo "=== done ==="
