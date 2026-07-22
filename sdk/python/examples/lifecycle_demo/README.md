# lifecycle_demo — recording script for "Workflow Lifecycle Policy In Action"

Replaces the old static GIF on the last slide. A workflow is running on a
stable version, gets manually rolled to a new version that ships a broken
tool, the operation detects the bad result and fails, and a workflow-lifecycle
policy automatically rolls it back — then you check the audit trail for why.

Tested end-to-end against the local stack: rollback fires ~10-15s after the
deploy with `BOUNDFLOW_PERIODIC_POLL_SECONDS=5`.

## One-time setup (before recording)

1. Speed up the scheduler's poll cycle so the demo doesn't sit idle for 30s
   between runs. `docker-compose.override.yml` (repo root, gitignored)
   already overrides `BOUNDFLOW_PERIODIC_POLL_SECONDS` to `5` on `server` and
   `scheduler` — Compose merges it automatically. Bring those services up (or
   restart them) so it takes effect:

   ```bash
   docker compose up -d --wait server scheduler
   ```

2. Provision an API key:

   ```bash
   docker compose run --rm server -mode=provision -name=lifecycle-demo
   export BOUNDFLOW_API_KEY=<the printed api_key>
   ```

3. Start the worker (its stdout is "the workflow itself's output" — show it
   on screen during the recording):

   ```bash
   cd sdk/python/examples/lifecycle_demo
   python worker.py
   ```

4. Get the workflow already running, before you hit record:

   ```bash
   ./setup.sh
   ```

   This creates + activates the workflow on v1 (repeating every 5s) and arms
   the rollback safety net policy (2 failures on the current version → roll
   back to v1). Prints the workflow id to `.demo_wf_id` for `run_demo.sh`.

## Recording

```bash
./run_demo.sh
```

Each step prints a header and waits for Enter, so you can talk over it live
in one take. It:

1. `workflow get` — shows it's already running on v1.
2. Shows `worker.py` — v1 (stable, `check_refund_eligibility`) vs v2 (ships a
   *new* tool, `check_refund_eligibility_v2`, that returns an invalid
   amount).
3. `workflow set-config <id> --version 2 --repeat 5` — the actual manual
   deploy. There's no separate "promote" button today; `set-config` is a
   full-replace of the workflow's config, so `--repeat 5` has to be passed
   again or the periodic cadence resets.
4. Waits ~15s: the worker terminal shows v2 running, the new tool returning
   an invalid amount, and the operation calling `ctx.mark_failed()` — twice.
5. `workflow get` — version is back to 1, rolled back automatically.
6. `audit workflow --json` — the fired rule: metric, threshold, trigger
   value, and the previous/target version. This is the "why."

Deletes the workflow and cleans up `.demo_wf_id` at the end.
