"""The words the console puts on screen that BoundFlow made up.

Only the console's own wording lives here — headings, column titles, the brand. A
downstream product can call a workflow an "agent" and a lifecycle a "runtime state"
without forking the console.

Deliberately absent: anything the control plane returns. `awaiting_approval`,
`cooldown`, `interrupted` and workflow types are stored values that also appear in
the CLI, the API and the audit log. Renaming those here would give an operator a word
that exists nowhere else — they report an agent stuck in "Needs sign-off" and nobody
can find it in any log or query. Show the stored value; rename the furniture.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Labels:
    """Console wording. Every field defaults to BoundFlow's own."""

    brand: str = "BoundFlow"
    tagline: str = "local operator console"

    # What the governed thing is called, and its two state columns. `workflow` heads
    # the id column, `workflow_type` the type beside it.
    workflow: str = "workflow"
    workflows: str = "workflows"
    workflow_type: str = "type"
    lifecycle: str = "lifecycle"
    state: str = "state"

    # One invocation of it.
    run: str = "run"
    runs: str = "runs"

    # Section headings, which double as the sidebar's nav entries.
    inbox: str = "Pending decisions"
    fleet: str = "Fleet"
    hold: str = "Operator hold"
    holds: str = "Holds"
    scheduling: str = "Scheduling"
    deleted: str = "Deleted"
    metrics: str = "Metrics"
    audit: str = "Audit"
    abandon: str = "Abandon queued runs"
    danger: str = "Delete"

    # Empty states, which read badly if they're built by concatenation.
    empty_inbox: str = "No decisions pending."
    empty_fleet: str = "No workflows yet."
    empty_runs: str = "No runs yet."
    empty_holds: str = "Nothing is held."
    empty_deleted: str = "Nothing deleted."


DEFAULT = Labels()
