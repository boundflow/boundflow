#!/usr/bin/env bash
# run_demo.sh — the recorded narrated walkthrough. Run ./setup.sh and start
# worker.py in another terminal FIRST (worker.py's stdout is "the workflow
# itself's output" — show it on screen), so the workflow is already running
# by the time you hit record.
#
# Each step waits for Enter so you can talk over it while recording.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .demo_wf_id ]; then
  echo "Run ./setup.sh first." >&2
  exit 1
fi
WF_ID=$(cat .demo_wf_id)

step() {
  echo
  echo "── $1 ──"
  read -rp "  [press enter] "
}

# ── 1. It's already running ──────────────────────────────────────────────────
step "1. workflow get — already running on v1"
boundflow workflow get "$WF_ID"

# ── 2. The workflow, in code ─────────────────────────────────────────────────
step "2. the workflow (worker.py) — v1 stable, v2 uses a new (broken) tool"
cat worker.py

# ── 3. Ship v2 ────────────────────────────────────────────────────────────────
step "3. set-config — manually roll it to v2 (this is the actual deploy action)"
boundflow workflow set-config "$WF_ID" --version 2 --repeat 5

# ── 4. Watch it break and self-heal ─────────────────────────────────────────────
step "4. switch to the worker terminal — v2 fails twice, then the safety net rolls it back. Give it ~15s, then continue"
sleep 15

# ── 5. Confirm the rollback ───────────────────────────────────────────────────
step "5. workflow get — version should read 1 again"
boundflow workflow get "$WF_ID"

# ── 6. Why it rolled back ─────────────────────────────────────────────────────
step "6. audit workflow — the fired rule (metric, threshold, trigger value, before/after version)"
boundflow --json audit workflow "$WF_ID" | python3 -m json.tool

echo
echo "=== done — cleaning up ==="
boundflow workflow delete "$WF_ID" --yes
rm -f .demo_wf_id
