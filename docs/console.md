# Operator console

The CLI and the SDK can read the fleet and act on it. The console is the same control
plane with a page in front of it, for the decisions that need a person and are
awkward to hand someone a terminal for — an approval gate needs whoever is qualified
to decide, not whoever has the CLI installed.

```bash
pip install "boundflow[ui]"
export BOUNDFLOW_API_KEY=<your key>
boundflow ui                    # serves http://127.0.0.1:8787
```

Four views, in the sidebar with live counts:

- **Fleet** — every live workflow with its lifecycle and workflow state, refreshing
  every four seconds. `/` filters the table, `j`/`k` move a cursor, `Enter` opens a
  row. Column headers sort; a tenant is a link that narrows the fleet to it. Both are
  server-side, so the refresh doesn't undo them.
- **Pending decisions** — approval gates with their justification, input gates with
  their prompt, each with `actor` and `reason` fields next to the decision. These are
  recorded on the audit event exactly as `--actor` and `--reason` are from the CLI.
- **Holds** — every workflow under an operator hold, with its release control. Resume
  only appears once a suspension has finished draining, which is when the control
  plane will accept it.
- **Deleted** — deleted but not yet purged. Deletion is soft plus a periodic purge, so
  these keep being returned for a while; they get their own view rather than swamping
  the fleet.

Opening a workflow gives its runs, its version metrics, its audit log, and the
control that applies to the state it is in — suspend, resume, activate (releasing a
lifecycle-policy pause), resolve (clearing a platform interruption), abandon queued
runs, or delete.

A workflow that isn't scheduling says why: held by an operator, stopped by a
lifecycle policy (with the metric, threshold and value that crossed, and when a
cooldown lifts), interrupted by a platform failure, or never activated.

The console is a client of the same control plane the CLI uses; it has no identity of
its own. Your API key stays in the `boundflow ui` process and is never rendered into
the page, and the server binds `127.0.0.1` only — anyone who can reach the console can
act with your key, so to reach a remote control plane point `--server` at it rather
than exposing the console:

```bash
boundflow --server https://boundflow.example.com:443 ui
```

Creating workflows, editing config and setting policy stay in the CLI, where a policy
is JSON you can review and commit rather than a form.
