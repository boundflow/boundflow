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

    # What the governed thing is called, and its two state columns.
    workflow: str = "workflow"
    workflows: str = "workflows"
    lifecycle: str = "lifecycle"
    state: str = "state"

    # One invocation of it.
    run: str = "run"
    runs: str = "runs"

    # Section headings.
    inbox: str = "Waiting on you"
    fleet: str = "Fleet"
    hold: str = "Operator hold"
    metrics: str = "Metrics"

    # Empty states, which read badly if they're built by concatenation.
    empty_inbox: str = "Nothing is waiting on a person."
    empty_fleet: str = "No workflows yet."
    empty_runs: str = "No runs yet."


DEFAULT = Labels()
