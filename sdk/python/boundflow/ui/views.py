"""Screen composition. Takes control-plane objects, returns HTML fragments.

Kept free of I/O so each screen can be rendered from a fixture in tests.
"""

from __future__ import annotations

from typing import Any

from ..control_plane import LifecycleState, WorkflowInfo, WorkflowState
from .render import detail_rows, esc, pill, table

# Workflows in one of these are waiting on a person, and are what the inbox is for.
GATED = (LifecycleState.AWAITING_APPROVAL, LifecycleState.AWAITING_INPUT)


def _link(workflow_id: str) -> str:
    return f'<a class="mono" href="/workflows/{esc(workflow_id)}">{esc(workflow_id)}</a>'


def fleet_table(workflows: list[WorkflowInfo]) -> str:
    rows = [
        [
            _link(w.id),
            esc(w.workflow_type),
            pill(w.lifecycle_state),
            pill(w.workflow_state),
            esc(w.version),
            esc(w.tenant_id),
        ]
        for w in workflows
    ]
    return table(
        ["workflow", "type", "lifecycle", "state", "version", "tenant"],
        rows,
        empty="No workflows. Create one with `boundflow workflows create`.",
    )


def _actor_reason_fields(reason_label: str) -> str:
    # actor is a free-text, self-asserted label, exactly as `--actor` is on the CLI.
    # The API key is the authentication; this is what gets recorded on the audit event.
    return (
        '<label>actor<input type="text" name="actor" placeholder="you@example.com"></label>'
        f'<label>{esc(reason_label)}'
        '<input type="text" name="reason" placeholder="recorded on the audit event"></label>'
    )


def approval_form(w: WorkflowInfo) -> str:
    a = w.pending_approval
    if a is None:
        return ""
    meta = f'<dt>metadata</dt><dd>{esc(a.metadata)}</dd>' if a.metadata else ""
    return (
        '<div class="card">'
        f"<dl><dt>approval</dt><dd class='mono'>{esc(a.approval_id)}</dd>"
        f"<dt>justification</dt><dd>{esc(a.justification)}</dd>"
        f"<dt>opened</dt><dd>{esc(a.opened_at)}</dd>"
        f"<dt>times out</dt><dd>{esc(a.timeout_at)}</dd>{meta}</dl>"
        f'<form method="post" action="/workflows/{esc(w.id)}/approval">'
        f'<input type="hidden" name="approval_id" value="{esc(a.approval_id)}">'
        f'{_actor_reason_fields("reason")}'
        '<button name="decision" value="approve">Approve</button>'
        '<button class="ghost" name="decision" value="reject">Reject</button>'
        "</form></div>"
    )


def input_form(w: WorkflowInfo) -> str:
    i = w.pending_input
    if i is None:
        return ""
    return (
        '<div class="card">'
        f"<dl><dt>input</dt><dd class='mono'>{esc(i.input_id)}</dd>"
        f"<dt>prompt</dt><dd>{esc(i.prompt)}</dd>"
        f"<dt>opened</dt><dd>{esc(i.opened_at)}</dd>"
        f"<dt>times out</dt><dd>{esc(i.timeout_at)}</dd></dl>"
        f'<form method="post" action="/workflows/{esc(w.id)}/input">'
        f'<input type="hidden" name="input_id" value="{esc(i.input_id)}">'
        '<label>answer<input type="text" name="answer" required '
        'placeholder=\'text, or {"key": "value"}\'></label>'
        '<label>actor<input type="text" name="actor" placeholder="you@example.com"></label>'
        '<button>Submit</button></form></div>'
    )


def inbox(gated: list[WorkflowInfo]) -> str:
    """Workflows parked on a human. The reason the console exists."""
    if not gated:
        return '<p class="muted">Nothing is waiting on a person.</p>'
    out = []
    for w in gated:
        out.append(
            f"<h3 style='margin:16px 0 4px;font-size:14px'>{_link(w.id)} "
            f"<span class='muted'>{esc(w.workflow_type)}</span></h3>"
            + approval_form(w)
            + input_form(w)
        )
    return "".join(out)


def suspension_controls(w: WorkflowInfo) -> str:
    """Suspend, or resume once a suspension has finished draining."""
    s = w.suspension
    if s is None:
        return (
            f'<form method="post" action="/workflows/{esc(w.id)}/suspend">'
            '<label>reason<input type="text" name="reason" '
            'placeholder="why the hold"></label>'
            '<label><input type="checkbox" name="stop_current" value="1"> '
            "stop the running run</label>"
            "<button>Suspend</button></form>"
        )
    draining = s.finalized_at is None
    detail = (
        f"<dl><dt>suspension</dt><dd class='mono'>{esc(s.suspension_id)}</dd>"
        f"<dt>reason</dt><dd>{esc(s.reason)}</dd>"
        f"<dt>requested</dt><dd>{esc(s.requested_at)}</dd>"
        f"<dt>finalized</dt><dd>{esc(s.finalized_at)}</dd></dl>"
    )
    if draining:
        # resume_workflow rejects a suspension that hasn't drained, so don't offer it.
        return detail + '<p class="muted">Draining — resumable once finalized.</p>'
    return (
        detail
        + f'<form method="post" action="/workflows/{esc(w.id)}/resume">'
        f'<input type="hidden" name="suspension_id" value="{esc(s.suspension_id)}">'
        "<button>Resume</button></form>"
    )


def runs_table(runs: list[Any]) -> str:
    rows = [
        [
            f'<span class="mono">{esc(r.request_id)}</span>',
            pill(r.status),
            pill(r.run_outcome) if r.run_outcome else '<span class="muted">—</span>',
            esc(r.failure_reason),
            esc(r.created_at),
            esc(r.completed_at),
        ]
        for r in runs
    ]
    return table(
        ["run", "status", "outcome", "failure", "created", "completed"],
        rows,
        empty="No runs yet.",
    )


def metrics_cards(m: Any) -> str:
    if m is None:
        return '<p class="muted">No metrics for this version yet.</p>'
    stats = [
        ("runs", m.run_count),
        ("cost usd", f"{m.total_cost_usd:,.4f}"),
        ("llm calls", m.total_llm_calls),
        ("failures", m.total_failures),
        ("rejections", m.total_approval_rejections),
        ("latency s", f"{m.total_latency_seconds:,.1f}"),
    ]
    cards = "".join(
        f'<div class="card stat"><b>{esc(v)}</b><span>{esc(k)}</span></div>'
        for k, v in stats
    )
    tools = (
        f'<p class="muted">tool failures: {esc(m.tool_failure_counts)}</p>'
        if m.tool_failure_counts
        else ""
    )
    return f'<div class="grid">{cards}</div>{tools}'


def home(workflows: list[WorkflowInfo], gated: list[WorkflowInfo]) -> str:
    counts: dict[str, int] = {}
    for w in workflows:
        counts[w.lifecycle_state.value] = counts.get(w.lifecycle_state.value, 0) + 1
    summary = "".join(
        f'<div class="card stat"><b>{esc(n)}</b><span>{esc(state)}</span></div>'
        for state, n in sorted(counts.items())
    )
    return (
        f"<h2>Waiting on you ({len(gated)})</h2>{inbox(gated)}"
        f'<h2>Fleet ({len(workflows)})</h2><div class="grid">{summary}</div>'
        f'<div id="fleet">{fleet_table(workflows)}</div>'
    )


def workflow_detail(w: WorkflowInfo, runs: list[Any], metrics: Any) -> str:
    gates = approval_form(w) + input_form(w)
    gates_section = f"<h2>Waiting on you</h2>{gates}" if gates else ""
    return (
        f"<h2>{esc(w.workflow_type)} <span class='mono muted'>{esc(w.id)}</span></h2>"
        f'<div class="card">{detail_rows(w, skip=("pending_approval", "pending_input"))}</div>'
        f"{gates_section}"
        f'<h2>Operator hold</h2><div class="card">{suspension_controls(w)}</div>'
        f"<h2>Metrics (version {esc(w.version)})</h2>{metrics_cards(metrics)}"
        f"<h2>Runs</h2>{runs_table(runs)}"
    )


def is_gated(w: WorkflowInfo) -> bool:
    return w.lifecycle_state in GATED


def is_suspended(w: WorkflowInfo) -> bool:
    return w.workflow_state == WorkflowState.SUSPENDED
