"""Screen composition. Takes control-plane objects, returns HTML fragments.

Kept free of I/O so each screen can be rendered from a fixture in tests.
"""

from __future__ import annotations

from typing import Any

from ..control_plane import LifecycleState, WorkflowInfo, WorkflowState
from .labels import DEFAULT, Labels
from .render import detail_rows, esc, fmt, pill, table

# Workflows in one of these are waiting on a person, and are what the inbox is for.
GATED = (LifecycleState.AWAITING_APPROVAL, LifecycleState.AWAITING_INPUT)


def _link(workflow_id: str) -> str:
    return f'<a class="mono" href="/workflows/{esc(workflow_id)}">{esc(workflow_id)}</a>'


# Fleet columns that can be sorted, mapped to the WorkflowInfo attribute behind them.
SORTABLE = {
    "workflow": "id",
    "type": "workflow_type",
    "lifecycle": "lifecycle_state",
    "state": "workflow_state",
    "version": "version",
    "tenant": "tenant_id",
}


def sort_workflows(workflows: list[WorkflowInfo], key: str = "",
                   desc: bool = False) -> list[WorkflowInfo]:
    """Sort by a fleet column. An unknown key leaves the order alone rather than
    raising — a hand-typed URL shouldn't take the page down."""
    attr = SORTABLE.get(key)
    if attr is None:
        return workflows
    return sorted(workflows, key=lambda w: fmt(getattr(w, attr)).lower(), reverse=desc)


def _header_link(label: str, col: str, sort: str, desc: bool, base: str) -> str:
    """A sortable header. Clicking the active column flips direction."""
    if col not in SORTABLE:
        return escape_header(label)
    flip = "1" if (sort == col and not desc) else ""
    mark = (" \u25be" if desc else " \u25b4") if sort == col else ""
    query = f"{base}sort={col}" + ("&desc=1" if flip else "")
    return f"<a href='{esc(query)}'>{escape_header(label)}{mark}</a>"


def escape_header(label: str) -> str:
    return esc(label)


def fleet_table(workflows: list[WorkflowInfo], lb: Labels = DEFAULT, *,
                sort: str = "", desc: bool = False, tenant: str = "") -> str:
    """The live fleet. Deleted workflows have their own view.

    Sorting and the tenant filter are server-side rather than JavaScript because the
    table reloads itself every few seconds — a client-side sort would be silently
    undone on the next poll, which is worse than not having one.
    """
    workflows = [w for w in workflows if not is_deleted(w)]
    if tenant:
        workflows = [w for w in workflows if w.tenant_id == tenant]
    workflows = sort_workflows(workflows, sort, desc)

    base = f"?tenant={tenant}&" if tenant else "?"
    rows = [
        [
            _link(w.id),
            esc(w.workflow_type),
            pill(w.lifecycle_state),
            pill(w.workflow_state),
            esc(w.version),
            # Clicking a tenant narrows the fleet to it; clicking again clears it.
            (f"<a href='?tenant='>{esc(w.tenant_id)}</a>" if tenant
             else f"<a href='?tenant={esc(w.tenant_id)}'>{esc(w.tenant_id)}</a>"),
        ]
        for w in workflows
    ]
    headers = [
        _header_link(lb.workflow, "workflow", sort, desc, base),
        _header_link(lb.workflow_type, "type", sort, desc, base),
        _header_link(lb.lifecycle, "lifecycle", sort, desc, base),
        _header_link(lb.state, "state", sort, desc, base),
        _header_link("version", "version", sort, desc, base),
        _header_link("tenant", "tenant", sort, desc, base),
    ]
    body = table(headers, rows, empty=lb.empty_fleet, raw_headers=True)
    if tenant:
        body = (f"<p class='muted'>Filtered to tenant <span class='mono'>"
                f"{esc(tenant)}</span> — <a href='?'>show all</a></p>{body}")
    return body


def _actor_reason_fields(reason_label: str) -> str:
    # actor is a free-text, self-asserted label, exactly as `--actor` is on the CLI.
    # The API key is the authentication; this is what gets recorded on the audit event.
    return (
        '<label>actor<input type="text" name="actor" placeholder="name@example.com"></label>'
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
        f"<dt>times out</dt><dd>{esc(i.timeout_at)}</dd>"
        + (f"<dt>metadata</dt><dd>{esc(i.metadata)}</dd>" if i.metadata else "")
        + "</dl>"
        f'<form method="post" action="/workflows/{esc(w.id)}/input">'
        f'<input type="hidden" name="input_id" value="{esc(i.input_id)}">'
        '<label>answer<input type="text" name="answer" required '
        'placeholder=\'text, or {"key": "value"}\'></label>'
        '<label>actor<input type="text" name="actor" placeholder="name@example.com"></label>'
        '<button>Submit</button></form></div>'
    )


def inbox(gated: list[WorkflowInfo], lb: Labels = DEFAULT) -> str:
    """Workflows parked on a human. The reason the console exists."""
    if not gated:
        return f'<p class="muted">{esc(lb.empty_inbox)}</p>'
    out = []
    for w in gated:
        out.append(
            f"<h3>{_link(w.id)} "
            f"<span class='muted'>{esc(w.workflow_type)}</span></h3>"
            + approval_form(w)
            + input_form(w)
        )
    return "".join(out)


def suspension_controls(w: WorkflowInfo, lb: Labels = DEFAULT) -> str:
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
    detail = (
        f"<dl><dt>suspension</dt><dd class='mono'>{esc(s.suspension_id)}</dd>"
        f"<dt>reason</dt><dd>{esc(s.reason)}</dd>"
        f"<dt>stop current run</dt><dd>{esc(s.stop_current)}</dd>"
        f"<dt>requested</dt><dd>{esc(s.requested_at)}</dd>"
        f"<dt>finalized</dt><dd>{esc(s.finalized_at)}</dd></dl>"
    )
    if s.finalized_at is None:
        # resume_workflow rejects a suspension that hasn't drained, so don't offer it.
        return detail + '<p class="muted">Draining — resumable once finalized.</p>'
    return (
        detail
        + f'<form method="post" action="/workflows/{esc(w.id)}/resume">'
        f'<input type="hidden" name="suspension_id" value="{esc(s.suspension_id)}">'
        "<button>Resume</button></form>"
    )


def suspend_control(w: WorkflowInfo, lb: Labels = DEFAULT) -> str:
    """Suspending needs a reason and a decision about the run in flight, so it's a
    block of its own beside delete rather than a button in the header — the two
    controls that take a deliberate act belong together, and away from the reading.

    Only offered where the operator is the one who can act: a policy-paused or
    disabled workflow isn't theirs to hold.
    """
    if w.workflow_state != WorkflowState.ACTIVE:
        return ""
    return (
        f'<div class="block warn"><strong>{esc(lb.hold)}</strong>'
        "<p>Stops new runs being scheduled. Anything queued is held, not lost, and "
        "comes back on resume.</p>"
        f'<form method="post" action="/workflows/{esc(w.id)}/suspend">'
        '<label>reason<input type="text" name="reason" '
        'placeholder="why the hold"></label>'
        '<label class="check"><input type="checkbox" name="stop_current" value="1"> '
        "stop the run in flight</label>"
        '<label>retarget (optional)<input type="text" name="suspension_id" '
        'placeholder="an existing suspension id"></label>'
        "<button class='ghost'>Suspend</button></form></div>"
    )


def actions(w: WorkflowInfo, lb: Labels = DEFAULT) -> str:
    """The state-changing controls, for the detail header.

    Only one is ever applicable, because which applies is decided by the state the
    workflow is actually in. Delete isn't here — it's irreversible, so it lives at
    the bottom, clear of anything reached while reading.
    """
    if w.lifecycle_state == LifecycleState.INTERRUPTED:
        return (
            f'<form method="post" action="/workflows/{esc(w.id)}/resolve">'
            '<input type="hidden" name="request_id" '
            f'value="{esc(w.last_interrupted_request_id)}">'
            "<button>Resolve &amp; reactivate</button></form>"
        )
    if w.workflow_state == WorkflowState.SUSPENDED:
        s = w.suspension
        if s is None or s.finalized_at is None:
            return ""            # still draining; resume would be refused
        return (
            f'<form method="post" action="/workflows/{esc(w.id)}/resume">'
            f'<input type="hidden" name="suspension_id" value="{esc(s.suspension_id)}">'
            "<button>Resume</button></form>"
        )
    if w.workflow_state in (WorkflowState.PAUSED, WorkflowState.COOLDOWN):
        # ActivateWorkflow is guarded on last_policy_decision_request_id, so sending
        # the decision currently on the workflow overrides exactly that one: if a
        # newer decision lands first this is refused rather than silently discarding
        # it. Empty for a workflow no policy has touched — the plain activation path.
        return (
            f'<form method="post" action="/workflows/{esc(w.id)}/activate">'
            '<input type="hidden" name="request_id" '
            f'value="{esc(w.last_policy_decision_request_id)}">'
            "<button>Activate</button></form>"
        )
    # Suspend isn't here: it needs a form, so it lives beside delete.
    return ""


def _callout(tone: str, title: str, detail: str) -> str:
    body = f"<p>{detail}</p>" if detail else ""
    return f'<div class="callout {tone}"><strong>{title}</strong>{body}</div>'


def policy_reason(record: Any) -> str:
    """One line saying what actually crossed, from a PolicyActionRecord.

    Without this the callout can say a policy acted but not why, which leaves the
    operator to go and find the rule themselves — the audit record already carries
    the metric, the threshold, the window and the value that crossed it.
    """
    if record is None:
        return ""
    window = f" over {record.window} runs" if record.window else ""
    return (f"{esc(record.metric)} reached {esc(record.trigger_value)} against a "
            f"threshold of {esc(record.threshold)}{esc(window)}.")


def status_callout(w: WorkflowInfo, lb: Labels = DEFAULT, policy: Any = None) -> str:
    """Why the workflow isn't scheduling — shown only when it isn't.

    workflow_state alone doesn't answer this: `paused` means a lifecycle policy
    stopped it, *or* that it was created and never activated, and neither reads any
    differently from the other. Naming the cause is the point. A workflow running
    normally says nothing here, because there is nothing to say.
    """
    if (w.deletion_requested_at is not None
            and w.lifecycle_state != LifecycleState.DELETED):
        return _callout("bad", "Deletion requested.",
                        "Waiting for anything in flight to finish before it is "
                        f"finalized. Requested {esc(w.deletion_requested_at)}.")
    if w.lifecycle_state == LifecycleState.DELETED:
        # Deletion is a soft delete plus an async purge, so a deleted workflow keeps
        # being listed until the reconciler collects it.
        return _callout("bad", "Deleted.",
                        "Waiting to be purged. It no longer schedules runs.")
    if w.lifecycle_state == LifecycleState.INTERRUPTED:
        return _callout("bad", "Stopped by a platform failure.",
                        f"Run <code>{esc(w.last_interrupted_request_id)}</code> was "
                        "interrupted before it could finish. Resolving clears it and "
                        "re-activates the workflow.")
    if w.workflow_state == WorkflowState.SUSPENDED:
        s = w.suspension
        if s is None:
            return _callout("warn", "Held by an operator.", "")
        note = ("Draining — resumable once the running run finishes."
                if s.finalized_at is None else "")
        reason = f"&ldquo;{esc(s.reason)}&rdquo;" if s.reason else "No reason given."
        return _callout("warn", "Held by an operator.", f"{reason} {note}".strip())
    if w.workflow_state in (WorkflowState.PAUSED, WorkflowState.COOLDOWN):
        # A freshly created workflow is `active`/`paused` — not `creating`, which is
        # what the shape of the enum suggests. So lifecycle_state can't distinguish
        # "never activated" from "a policy stopped it"; the presence of a policy
        # decision is what does. Getting this wrong reported a brand new workflow as
        # having been paused by a policy acting on metrics it has never produced.
        if not w.last_policy_decision_request_id:
            return _callout("warn", "Not activated.",
                            "No policy has acted on it and it is not yet eligible "
                            "to run.")
        if w.workflow_state == WorkflowState.COOLDOWN:
            until = (f" Scheduling resumes {fmt(w.cooldown_until)}."
                     if w.cooldown_until else "")
            verb = f"Cooling down after a lifecycle policy decision.{until}"
        else:
            verb = "Paused by a lifecycle policy."
        why = policy_reason(policy)
        detail = (f"{why} " if why else
                  "The platform acted on this workflow's own metrics, not an "
                  "operator. ")
        return _callout(
            "warn", verb,
            f"{detail}Triggered by run <code>"
            f"{esc(w.last_policy_decision_request_id)}</code>. "
            "Activating overrides that decision.")
    if w.workflow_state == WorkflowState.DISABLED:
        return _callout("bad", "Disabled.", "It will not schedule runs.")
    return ""


def abandon_control(w: WorkflowInfo, lb: Labels = DEFAULT) -> str:
    """Drop queued runs, by id or all of them.

    Only queued runs can be abandoned — one already scheduled or in progress is
    untouched — so this is safe at any time, but it is irreversible for the runs it
    does catch, which is why it sits down here rather than beside the runs table.
    """
    return (
        f'<div class="block warn"><strong>{esc(lb.abandon)}</strong>'
        "<p>Drops runs still waiting in the queue. A run already scheduled or in "
        "progress is untouched. Irreversible for the runs it catches.</p>"
        f'<form method="post" action="/workflows/{esc(w.id)}/abandon">'
        '<label>run ids<input type="text" name="request_ids" '
        'placeholder="comma separated; leave empty for all"></label>'
        '<label class="check"><input type="checkbox" name="all" value="1"> '
        "all queued runs</label>"
        "<button class='ghost'>Abandon</button></form></div>"
    )


def delete_control(w: WorkflowInfo, lb: Labels = DEFAULT) -> str:
    """Irreversible, and this console has no undo — so it sits apart from everything
    else and asks for the id to be typed."""
    if w.deletion_requested_at is not None:
        return (f'<div class="danger"><strong>{esc(lb.danger)}</strong>'
                '<p class="muted">Deletion already requested at '
                f"{esc(w.deletion_requested_at)}.</p></div>")
    return (
        f'<div class="danger"><strong>{esc(lb.danger)}</strong>'
        "<p>Deletes the workflow and abandons anything queued for it. "
        "Type the id to confirm.</p>"
        f'<form method="post" action="/workflows/{esc(w.id)}/delete">'
        '<label>workflow id<input type="text" name="confirm" required '
        'autocomplete="off" placeholder="paste the id above"></label>'
        "<button class='ghost'>Delete</button></form></div>"
    )


def runs_table(runs: list[Any], lb: Labels = DEFAULT) -> str:
    rows = [
        [
            f'<span class="mono">{esc(r.request_id)}</span>',
            esc(r.request_type),
            pill(r.status),
            pill(r.run_outcome) if r.run_outcome else '<span class="muted">—</span>',
            esc(r.failure_reason),
            esc(r.created_at),
            esc(r.completed_at),
        ]
        for r in runs
    ]
    return table(
        [lb.run, "type", "status", "outcome", "failure", "created", "completed"],
        rows,
        empty=lb.empty_runs,
    )


def metrics_cards(m: Any, lb: Labels = DEFAULT) -> str:
    if m is None:
        return f'<p class="muted">No {esc(lb.metrics).lower()} for this version yet.</p>'
    stats = [
        (lb.runs, m.run_count),
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


def home(workflows: list[WorkflowInfo], gated: list[WorkflowInfo],
         lb: Labels = DEFAULT, *, sort: str = "", desc: bool = False,
         tenant: str = "") -> str:
    """The fleet, with anything parked on a person pulled to the top."""
    head = f"<h2>{esc(lb.inbox)} ({len(gated)})</h2>{inbox(gated, lb)}" if gated else ""
    live = [w for w in workflows if not is_deleted(w)
            and (not tenant or w.tenant_id == tenant)]
    # The poll re-fetches with the same sort and filter, so neither is lost on refresh.
    src = f"/fragment/fleet?sort={sort}&desc={'1' if desc else ''}&tenant={tenant}"
    return (
        f"{head}<h2>{esc(lb.fleet)} ({len(live)})</h2>"
        f'<div id="fleet" data-src="{esc(src)}">'
        f"{fleet_table(workflows, lb, sort=sort, desc=desc, tenant=tenant)}</div>"
    )


def inbox_page(gated: list[WorkflowInfo], lb: Labels = DEFAULT) -> str:
    return f"<h2>{esc(lb.inbox)} ({len(gated)})</h2>{inbox(gated, lb)}"


def deleted_page(gone: list[WorkflowInfo], lb: Labels = DEFAULT) -> str:
    """Workflows that have been deleted but not yet purged.

    Deletion is a soft delete plus a periodic purge, so these keep being returned by
    the control plane for a while. Somewhere to look for them beats both hiding them
    (where did it go?) and leaving them in the fleet (which they swamp).
    """
    rows = [
        [
            _link(w.id),
            esc(w.workflow_type),
            pill(w.lifecycle_state),
            esc(w.deletion_requested_at),
            esc(w.tenant_id),
        ]
        for w in gone
    ]
    return (f"<h2>{esc(lb.deleted)} ({len(gone)})</h2>"
            + table([lb.workflow, lb.workflow_type, lb.lifecycle,
                     "deletion requested", "tenant"],
                    rows, empty=lb.empty_deleted))


def holds_page(held: list[WorkflowInfo], lb: Labels = DEFAULT) -> str:
    """Every workflow under an operator hold, each with its release control."""
    if not held:
        return f'<p class="muted">{esc(lb.empty_holds)}</p>'
    out = [f"<h2>{esc(lb.hold)} ({len(held)})</h2>"]
    for w in held:
        out.append(
            f"<h3>{_link(w.id)} <span class='muted'>{esc(w.workflow_type)}</span></h3>"
            f'<div class="card">{suspension_controls(w, lb)}</div>'
        )
    return "".join(out)


def _audit_summary(r: Any) -> tuple[str, str]:
    """(kind, one-line summary) for any of the four audit record types."""
    cls = type(r).__name__
    if cls == "ApprovalAuditRecord":
        bits = [f"“{r.justification}”" if r.justification else "",
                f"— {r.reason}" if r.reason else ""]
        return "approval", f"{fmt(r.decision)} {' '.join(b for b in bits if b)}".strip()
    if cls == "InputAuditRecord":
        return "input", f"{fmt(r.decision)} “{r.prompt}” → {fmt(r.answer)}".strip()
    if cls == "PolicyActionRecord":
        return "workflow policy", f"{fmt(r.action)}: {policy_reason(r)}"
    if cls == "AgentPolicyActionRecord":
        n = len(r.fired_rules)
        return "agent policy", f"{r.agent}: {n} rule{'s' if n != 1 else ''} fired"
    return cls, fmt(r)


def audit_table(records: list[Any], lb: Labels = DEFAULT) -> str:
    """The unified audit log for one workflow — every decision, by whom, and why.

    Every gate decision and policy firing is already recorded server-side; a console
    that shows current state but not how it got there sends an operator to the CLI
    for the question they most often have.
    """
    rows = []
    for r in records:
        kind, summary = _audit_summary(r)
        # The summary is what you scan; the record is what you need once something
        # looks wrong, so every field stays available rather than being summarised
        # away. Approval ids, gate timings and the policy's previous state are all
        # here for a reason.
        full = (f"<details><summary>{esc(summary)}</summary>"
                f"{detail_rows(r, skip=('workflow_id', 'request_id', 'actor', 'occurred_at'))}"
                "</details>")
        rows.append([
            esc(getattr(r, "occurred_at", None)),
            esc(kind),
            # Every record type carries the run it came from, which ties a decision
            # back to the run in the table above it.
            f'<span class="mono">{esc(getattr(r, "request_id", ""))}</span>',
            esc(getattr(r, "actor", "") or "—"),
            full,
        ])
    return table(["when", "kind", "run", "actor", "what"], rows,
                 empty="Nothing recorded yet.")


def workflow_detail(w: WorkflowInfo, runs: list[Any], metrics: Any,
                    lb: Labels = DEFAULT, audit: list[Any] | None = None,
                    policy: Any = None) -> str:
    gates = approval_form(w) + input_form(w)
    gates_section = f"<h2>{esc(lb.inbox)}</h2>{gates}" if gates else ""
    return (
        "<div class='dhead'>"
        f"<h2>{esc(w.workflow_type)} <span class='mono muted'>{esc(w.id)}</span></h2>"
        f"<div class='actions'>{actions(w, lb)}</div></div>"
        f"{status_callout(w, lb, policy)}"
        f"{gates_section}"
        f'<div class="card">{detail_rows(w, skip=("pending_approval", "pending_input"))}</div>'
        f"<h2>{esc(lb.metrics)} (version {esc(w.version)})</h2>"
        f"{metrics_cards(metrics, lb)}"
        f"<h2>{esc(lb.runs).capitalize()}</h2>{runs_table(runs, lb)}"
        f"<h2>{esc(lb.audit)}</h2>{audit_table(audit or [], lb)}"
        f"{suspend_control(w, lb)}{abandon_control(w, lb)}"
        f"{delete_control(w, lb)}"
    )


def is_gated(w: WorkflowInfo) -> bool:
    return w.lifecycle_state in GATED


def is_suspended(w: WorkflowInfo) -> bool:
    return w.workflow_state == WorkflowState.SUSPENDED


def is_deleted(w: WorkflowInfo) -> bool:
    """Finalized deletions only. A workflow whose deletion was merely *requested* is
    still draining — possibly still running — and belongs in the fleet."""
    return w.lifecycle_state == LifecycleState.DELETED
