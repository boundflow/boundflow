"""The operator console, driven against a fake control plane.

The console is a client of ControlPlaneClient and nothing else, so a stand-in with the
same method names exercises every screen and action without a backend. What's worth
pinning here is the console's own logic: which workflows reach the inbox, that a gate
decision reaches the right RPC with the operator's actor/reason, and that user text
can't inject markup.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from boundflow.control_plane import (
    LifecycleState,
    PendingApproval,
    PendingInput,
    Run,
    RunOutcome,
    RunStatus,
    Suspension,
    WorkflowInfo,
    WorkflowMetrics,
    WorkflowState,
)
from boundflow.ui import views
from boundflow.ui.server import Console, _parse_answer, build_app

starlette = pytest.importorskip("starlette")
from starlette.testclient import TestClient  # noqa: E402

NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)


def _wf(wid, *, lifecycle=LifecycleState.ACTIVE, state=WorkflowState.ACTIVE,
        approval=None, pending_input=None, suspension=None, wtype="leads_finder"):
    return WorkflowInfo(
        id=wid, workflow_type=wtype, tenant_id="t1", lifecycle_state=lifecycle,
        workflow_state=state, version=1, last_interrupted_request_id="",
        last_policy_decision_request_id="", pending_approval=approval,
        pending_input=pending_input, suspension=suspension,
    )


class FakeCP:
    """Records calls; returns whatever the test set up."""

    def __init__(self, workflows, audit=None):
        self._workflows = {w.id: w for w in workflows}
        self.calls: list[tuple] = []
        self.audit = audit or []

    async def list_workflows(self):
        # Mirrors the real light view: gate detail is only on get_workflow.
        return [
            WorkflowInfo(**{**w.__dict__, "pending_approval": None,
                            "pending_input": None, "config": None})
            for w in self._workflows.values()
        ]

    async def get_workflow(self, wid):
        return self._workflows[wid]

    async def list_workflow_runs(self, wid):
        return [Run("r1", "invoke", RunStatus.COMPLETED, None, "", NOW, NOW)]

    async def get_workflow_metrics(self, wid):
        return WorkflowMetrics(1, 1.5, 3, 0, 9, 12.0, 0, {})

    async def get_audit_log(self, wid=""):
        return self.audit

    async def get_workflow_policy_audit(self, wid):
        return [r for r in self.audit if type(r).__name__ == "PolicyActionRecord"]

    async def approve_workflow(self, wid, aid, actor="", reason=""):
        self.calls.append(("approve", wid, aid, actor, reason))

    async def reject_workflow(self, wid, aid, actor="", reason=""):
        self.calls.append(("reject", wid, aid, actor, reason))

    async def submit_input(self, wid, iid, answer, actor=""):
        self.calls.append(("input", wid, iid, answer, actor))

    async def suspend_workflow(self, wid, reason="", stop_current_run=False,
                               suspension_id=""):
        self.calls.append(("suspend", wid, reason, stop_current_run))
        return "sus-1"

    async def resume_workflow(self, wid, sid):
        self.calls.append(("resume", wid, sid))


def client(workflows, audit=None):
    console = Console("http://localhost:50051", "key")
    cp = FakeCP(workflows, audit)
    console._cp = cp
    # console._cp is already set, and TestClient only runs the lifespan inside a
    # `with` block, so the fake survives.
    return TestClient(build_app(console)), cp


APPROVAL = PendingApproval("ap-1", "refund over $500", {}, NOW, None)
INPUT_GATE = PendingInput("in-1", "which region?", {}, NOW, None)


def test_inbox_holds_only_workflows_waiting_on_a_person():
    """The inbox is the console's reason to exist, so what lands in it is the one
    piece of routing worth pinning."""
    c, _ = client([
        _wf("w-gated", lifecycle=LifecycleState.AWAITING_APPROVAL, approval=APPROVAL),
        _wf("w-running"),
        _wf("w-broken", lifecycle=LifecycleState.INTERRUPTED),
    ])
    body = c.get("/").text
    assert "Pending decisions (1)" in body
    assert "refund over $500" in body
    assert "w-running" in body       # still on the fleet table
    assert "ap-1" in body


def test_a_workflow_that_leaves_its_gate_drops_out_of_the_inbox():
    """list_workflows and get_workflow are two calls; a gate can close in between."""
    moved = _wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL)
    moved.lifecycle_state = LifecycleState.AWAITING_APPROVAL
    c, cp = client([moved])
    # get_workflow reports it has moved on, with no gate attached.
    cp._workflows["w1"] = _wf("w1", lifecycle=LifecycleState.ACTIVE)
    home = c.get("/").text
    assert "Pending decisions (" not in home    # the section is dropped entirely
    assert "Pending decisions<b>0</b>" in home  # the sidebar still says zero
    assert "No decisions pending." in c.get("/inbox").text


def test_approve_sends_actor_and_reason_then_redirects():
    c, cp = client([_wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL,
                        approval=APPROVAL)])
    r = c.post("/workflows/w1/approval",
               data={"decision": "approve", "approval_id": "ap-1",
                     "actor": "arjun@boundflow.dev", "reason": "under limit"},
               follow_redirects=False)
    assert r.status_code == 303          # so a refresh can't re-decide
    assert cp.calls == [("approve", "w1", "ap-1", "arjun@boundflow.dev", "under limit")]


def test_reject_routes_to_the_other_rpc():
    c, cp = client([_wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL,
                        approval=APPROVAL)])
    c.post("/workflows/w1/approval",
           data={"decision": "reject", "approval_id": "ap-1", "actor": "a", "reason": "no"},
           follow_redirects=False)
    assert cp.calls[0][0] == "reject"


def test_a_failed_decision_surfaces_instead_of_vanishing():
    c, cp = client([_wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL,
                        approval=APPROVAL)])

    async def boom(*a, **k):
        raise RuntimeError("approval already resolved")

    cp.approve_workflow = boom
    r = c.post("/workflows/w1/approval",
               data={"decision": "approve", "approval_id": "ap-1"},
               follow_redirects=False)
    assert "approval+already+resolved" in r.headers["location"].replace("%20", "+")


def test_input_answer_accepts_json_or_plain_text():
    assert _parse_answer('{"choice": "refund"}') == {"choice": "refund"}
    # A plain answer is wrapped rather than rejected — the field says so.
    assert _parse_answer("us-east") == {"answer": "us-east"}
    assert _parse_answer("[1,2]") == {"answer": [1, 2]}


def test_submit_input_reaches_the_rpc():
    c, cp = client([_wf("w1", lifecycle=LifecycleState.AWAITING_INPUT,
                        pending_input=INPUT_GATE)])
    c.post("/workflows/w1/input",
           data={"input_id": "in-1", "answer": "us-east", "actor": "arjun"},
           follow_redirects=False)
    assert cp.calls == [("input", "w1", "in-1", {"answer": "us-east"}, "arjun")]


def test_suspend_and_resume():
    c, cp = client([_wf("w1")])
    c.post("/workflows/w1/suspend", data={"reason": "cost spike", "stop_current": "1"},
           follow_redirects=False)
    assert cp.calls == [("suspend", "w1", "cost spike", True)]


def test_resume_is_not_offered_while_a_suspension_is_draining():
    """resume_workflow rejects a suspension that hasn't finalized, so the button
    shouldn't be there to click."""
    draining = Suspension("sus-1", "cost spike", True, NOW, None)
    assert "Resume" not in views.suspension_controls(
        _wf("w1", state=WorkflowState.SUSPENDED, suspension=draining))

    drained = Suspension("sus-1", "cost spike", True, NOW, NOW)
    assert "Resume" in views.suspension_controls(
        _wf("w1", state=WorkflowState.SUSPENDED, suspension=drained))


def test_user_text_cannot_inject_markup():
    """Justifications, reasons and workflow types are all user-supplied and reach
    the page directly."""
    nasty = PendingApproval("ap-1", "<script>alert(1)</script>", {}, NOW, None)
    html = views.approval_form(_wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL,
                                   approval=nasty))
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_detail_page_renders_runs_and_metrics():
    c, _ = client([_wf("w1")])
    body = c.get("/workflows/w1").text
    assert "leads_finder" in body
    assert "r1" in body            # the run
    assert "1.5000" in body        # total cost


def test_detail_survives_metrics_being_unavailable():
    """A workflow with no metrics yet still has to render — the page is how you find
    out what's wrong with it."""
    c, cp = client([_wf("w1")])

    async def boom(wid):
        raise RuntimeError("no metrics for version")

    cp.get_workflow_metrics = boom
    r = c.get("/workflows/w1")
    assert r.status_code == 200
    assert "No metrics" in r.text


def test_fleet_fragment_keeps_the_last_good_table_on_error():
    c, cp = client([_wf("w1")])

    async def boom():
        raise RuntimeError("control plane down")

    cp.list_workflows = boom
    assert c.get("/fragment/fleet").status_code == 502


def test_labels_rename_the_console_s_own_words():
    from boundflow.ui import Labels

    console = Console("http://localhost:50051", "key",
                      Labels(brand="Acme", tagline="agent control",
                             workflow="agent", workflows="agents",
                             lifecycle="runtime state", fleet="Agents",
                             inbox="Needs you", runs="invocations"))
    console._cp = FakeCP([_wf("w1")])
    body = TestClient(build_app(console)).get("/").text

    assert "Acme" in body and "agent control" in body
    assert "Needs you<b>0</b>" in body           # sidebar
    assert "Agents (1)" in body
    assert "runtime state" in body
    assert "BoundFlow" not in body
    assert "Pending decisions" not in body


def test_labels_name_both_the_id_column_and_the_type_beside_it():
    """A product whose workflow type is what its operators name — an agent, a job —
    needs the type column too. With one label the id column carries the word and the
    column actually holding the name is headed "type"."""
    from boundflow.ui import Labels

    console = Console("http://localhost:50051", "key",
                      Labels(workflow="instance", workflow_type="agent"))
    console._cp = FakeCP([_wf("w1")])
    body = TestClient(build_app(console)).get("/").text

    assert ">instance<" in body
    assert ">agent<" in body
    assert ">type<" not in body


def test_the_keyboard_hint_uses_the_console_s_word_for_the_fleet():
    """The footer offers `g` to reach the fleet, so it has to call it what the
    sidebar does — otherwise the nav says Agents and the hint says fleet."""
    from boundflow.ui import Labels

    console = Console("http://localhost:50051", "key", Labels(fleet="Agents"))
    console._cp = FakeCP([_wf("w1")])
    body = TestClient(build_app(console)).get("/").text

    assert "</kbd> Agents</span>" in body
    assert "</kbd> fleet</span>" not in body


def test_every_table_calls_a_run_what_the_labels_call_it():
    """The audit table headed its run column by hand, so a console that renames the
    run said task in the run table and run in the audit below it."""
    from boundflow.ui import Labels

    console = Console("http://localhost:50051", "key", Labels(run="task", runs="tasks"))
    console._cp = FakeCP([_wf("w1")])
    body = TestClient(build_app(console)).get("/workflows/w1").text

    assert ">run<" not in body


def test_labels_cannot_rename_what_the_control_plane_returns():
    """The stored values are what appear in the CLI, the API and the audit log. A
    console word that exists nowhere else makes an operator's report unsearchable."""
    from boundflow.ui import Labels

    console = Console("http://localhost:50051", "key",
                      Labels(workflow="agent", lifecycle="runtime state"))
    console._cp = FakeCP([_wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL,
                              approval=APPROVAL, wtype="leads_finder")])
    body = TestClient(build_app(console)).get("/").text

    assert "awaiting_approval" in body     # the state, verbatim
    assert "leads_finder" in body          # the workflow type, verbatim


def test_labels_are_escaped():
    from boundflow.ui import Labels

    console = Console("http://localhost:50051", "key",
                      Labels(brand="<script>alert(1)</script>"))
    console._cp = FakeCP([_wf("w1")])
    body = TestClient(build_app(console)).get("/").text
    assert "<script>alert(1)</script>" not in body


def test_sidebar_counts_the_three_views():
    c, _ = client([
        _wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL, approval=APPROVAL),
        _wf("w2"),
        _wf("w3", state=WorkflowState.SUSPENDED,
            suspension=Suspension("s1", "hold", False, NOW, NOW)),
    ])
    body = c.get("/").text
    assert "Fleet<b>3</b>" in body
    assert "Pending decisions<b>1</b>" in body
    assert "Holds<b>1</b>" in body


def test_holds_view_lists_held_workflows_with_their_release():
    drained = Suspension("sus-1", "cost spike", True, NOW, NOW)
    c, _ = client([
        _wf("w1", state=WorkflowState.SUSPENDED, suspension=drained),
        _wf("w2"),
    ])
    body = c.get("/holds").text
    assert "cost spike" in body
    assert "Resume" in body
    assert "w2" not in body        # not held, so not here


def test_holds_view_is_empty_when_nothing_is_held():
    c, _ = client([_wf("w1")])
    assert "Nothing is held." in c.get("/holds").text


def test_inbox_view_shows_only_the_gates():
    c, _ = client([
        _wf("w1", lifecycle=LifecycleState.AWAITING_INPUT, pending_input=INPUT_GATE),
        _wf("w2"),
    ])
    body = c.get("/inbox").text
    assert "which region?" in body
    assert "w2" not in body


def test_a_view_that_cannot_reach_the_control_plane_still_renders_its_nav():
    """The error page is where an operator lands when things are broken, so it has to
    keep the navigation rather than becoming a dead end."""
    c, cp = client([_wf("w1")])

    async def boom():
        raise RuntimeError("control plane down")

    cp.list_workflows = boom
    body = c.get("/").text
    assert "control plane down" in body
    assert "href='/holds'" in body


def test_fleet_polls_itself_and_the_detail_page_does_not():
    c, _ = client([_wf("w1")])
    # The src now carries the sort and tenant filter so a poll preserves them.
    assert 'data-src="/fragment/fleet' in c.get("/").text
    assert "data-src=" not in c.get("/workflows/w1").text


def test_screens_are_never_cached():
    """A cached gate invites a decision on an approval that is already resolved."""
    c, _ = client([_wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL,
                       approval=APPROVAL)])
    for path in ("/", "/inbox", "/holds", "/workflows/w1", "/fragment/fleet"):
        assert c.get(path).headers["cache-control"] == "no-store", path


# ── Scheduling: naming the cause ─────────────────────────────────────────────
# workflow_state alone can't answer "why isn't this running" — `paused` covers both
# a policy decision and a workflow nobody ever activated. These pin that the console
# distinguishes them, since that was invisible when both rendered as the same pill.

def _sched(w):
    """The callout plus whatever control that state offers."""
    return views.status_callout(w) + views.actions(w) + views.suspend_control(w)


def test_an_operator_hold_reads_as_an_operator_hold():
    held = _wf("w1", lifecycle=LifecycleState.HALTED, state=WorkflowState.SUSPENDED,
               suspension=Suspension("s1", "cost spike", True, NOW, NOW))
    body = _sched(held)
    assert "Held by an operator" in body
    assert "cost spike" in body
    assert "Resume" in body
    assert "lifecycle policy" not in body


def test_a_policy_pause_reads_as_a_policy_decision_not_an_operator():
    paused = _wf("w1", lifecycle=LifecycleState.BLOCKED, state=WorkflowState.PAUSED)
    paused.last_policy_decision_request_id = "req-77"
    body = _sched(paused)
    assert "lifecycle policy" in body
    assert "not an operator" in body
    assert "req-77" in body          # the thread to pull
    assert "Held by an operator" not in body


def test_cooldown_says_cooling_down_rather_than_paused():
    cooling = _wf("w1", lifecycle=LifecycleState.BLOCKED, state=WorkflowState.COOLDOWN)
    cooling.last_policy_decision_request_id = "req-9"
    assert "Cooling down" in _sched(cooling)


def test_a_deleted_workflow_says_so_rather_than_looking_idle():
    """Soft delete plus async purge means it keeps being listed for a while."""
    gone = _wf("w1", lifecycle=LifecycleState.DELETED, state=WorkflowState.DISABLED)
    assert "Deleted." in views.status_callout(gone)


def test_a_never_activated_workflow_is_not_reported_as_policy_paused():
    """A fresh workflow is lifecycle=active, workflow_state=paused — the same
    workflow_state a policy sets, and *not* `creating`. Only the absence of a policy
    decision separates them, so getting this wrong told an operator a brand new
    workflow had been stopped by a policy acting on metrics it never produced."""
    fresh = _wf("w1", lifecycle=LifecycleState.ACTIVE, state=WorkflowState.PAUSED)
    fresh.last_policy_decision_request_id = ""
    body = _sched(fresh)
    assert "Not activated." in body
    assert "lifecycle policy" not in body


def test_an_interruption_offers_resolve_with_the_run_id_filled_in():
    """The CLI makes you find and paste last_interrupted_request_id; it's already on
    the workflow, so the console shouldn't."""
    broken = _wf("w1", lifecycle=LifecycleState.INTERRUPTED,
                 state=WorkflowState.DISABLED)
    broken.last_interrupted_request_id = "req-dead"
    body = _sched(broken)
    assert "platform failure" in body
    assert 'value="req-dead"' in body
    assert "Resolve" in body


def test_an_active_workflow_says_nothing_but_still_offers_the_hold():
    """The old version printed "Scheduling normally" — a section whose only job was
    to report that there was nothing to report."""
    w = _wf("w1")
    assert views.status_callout(w) == ""        # no callout when nothing is wrong
    assert "Suspend" in views.suspend_control(w)


def test_resolve_reaches_the_rpc():
    c, cp = client([_wf("w1", lifecycle=LifecycleState.INTERRUPTED)])

    async def resolve(wid, rid):
        cp.calls.append(("resolve", wid, rid))

    cp.resolve_interrupted_workflow = resolve
    c.post("/workflows/w1/resolve", data={"request_id": "req-dead"},
           follow_redirects=False)
    assert cp.calls == [("resolve", "w1", "req-dead")]


# ── Delete ───────────────────────────────────────────────────────────────────

def test_delete_requires_the_typed_id_to_match():
    c, cp = client([_wf("w1")])
    deleted = []
    cp.delete_workflow = lambda wid: deleted.append(wid)

    r = c.post("/workflows/w1/delete", data={"confirm": "w2"}, follow_redirects=False)
    assert deleted == []                                  # nothing happened
    assert "does+not+match" in r.headers["location"].replace("%20", "+")


def test_delete_with_the_right_id_deletes_and_leaves_the_workflow():
    c, cp = client([_wf("w1")])
    deleted = []

    async def go(wid):
        deleted.append(wid)

    cp.delete_workflow = go
    r = c.post("/workflows/w1/delete", data={"confirm": "w1"}, follow_redirects=False)
    assert deleted == ["w1"]
    # Redirecting back to the workflow would 404 — it's gone.
    assert r.headers["location"] == "/"


def test_delete_is_offered_on_the_detail_page_only():
    """A delete button in a list, next to a 4-second auto-refresh, is a misclick."""
    c, _ = client([_wf("w1")])
    assert "/workflows/w1/delete" in c.get("/workflows/w1").text
    # Not `/delete` — the Deleted view's nav href contains that as a substring.
    assert "/workflows/w1/delete" not in c.get("/").text


def test_a_workflow_already_being_deleted_is_not_offered_again():
    w = _wf("w1")
    w.deletion_requested_at = NOW
    body = views.delete_control(w)
    assert "already requested" in body
    assert "<form" not in body


def test_nested_objects_render_as_lists_not_a_flattened_line():
    """A workflow's config and suspension are where the detail matters most, and
    comma-joining them produced a repr rather than something readable."""
    from boundflow.control_plane import InvokeMode, WorkflowConfig
    from boundflow.ui.render import detail_rows

    w = _wf("w1", suspension=Suspension("s1", "cost spike", True, NOW, NOW))
    w.config = WorkflowConfig(1, 300, 0, True, InvokeMode.COALESCE, 0, True)
    html = detail_rows(w)

    assert "invoke_timeout_seconds=300" not in html      # the old flattened form
    assert html.count('<dl class="sub">') == 2           # config and suspension
    assert "<dt>invoke timeout seconds</dt><dd>300</dd>" in html
    assert "<dt>suspension id</dt>" in html


def test_an_empty_dict_stays_inline():
    """Only non-empty objects earn their own list; an empty one is just a dash."""
    from boundflow.ui.render import detail_rows

    w = _wf("w1")
    html = detail_rows(w)
    assert '<dl class="sub">' not in html


def test_only_the_action_that_applies_is_offered():
    """Which control shows is decided by the state, so there is never a Resume next
    to a Suspend for the operator to choose wrongly between."""
    assert views.actions(_wf("w1")) == ""       # suspend is a block, not a header button

    drained = views.actions(_wf("w1", state=WorkflowState.SUSPENDED,
                                suspension=Suspension("s1", "r", False, NOW, NOW)))
    assert "Resume" in drained

    draining = views.actions(_wf("w1", state=WorkflowState.SUSPENDED,
                                 suspension=Suspension("s1", "r", False, NOW, None)))
    assert draining == ""                       # resume would be refused

    paused = _wf("w1", lifecycle=LifecycleState.BLOCKED, state=WorkflowState.PAUSED)
    assert "Activate" in views.actions(paused)  # releasing a policy pause
    assert views.suspend_control(paused) == ""  # but it can't be held on top of that


def test_delete_is_not_among_the_header_actions():
    """Irreversible, so it must not sit where you click while reading."""
    assert "delete" not in views.actions(_wf("w1")).lower()


def test_a_policy_pause_offers_activation_carrying_that_decision():
    """The callout used to name a cause and offer nothing to do about it.

    ActivateWorkflow is guarded on last_policy_decision_request_id, so sending the
    decision currently on the workflow overrides exactly that one — if a newer
    decision lands first the click is refused rather than quietly discarding it.
    """
    paused = _wf("w1", lifecycle=LifecycleState.BLOCKED, state=WorkflowState.PAUSED)
    paused.last_policy_decision_request_id = "req-77"
    assert 'value="req-77"' in views.actions(paused)
    assert "overrides that decision" in views.status_callout(paused)


def test_activate_reaches_the_rpc_with_the_decision_id():
    c, cp = client([_wf("w1", lifecycle=LifecycleState.BLOCKED,
                        state=WorkflowState.PAUSED)])

    async def go(wid, rid=""):
        cp.calls.append(("activate", wid, rid))

    cp.activate_workflow = go
    c.post("/workflows/w1/activate", data={"request_id": "req-77"},
           follow_redirects=False)
    assert cp.calls == [("activate", "w1", "req-77")]


def test_no_second_person_copy_anywhere():
    """This is infrastructure tooling, not a demo talking to its user."""
    import re
    from boundflow.ui import labels, views

    c, _ = client([_wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL,
                       approval=APPROVAL)])
    surfaces = [c.get(p).text for p in ("/", "/inbox", "/holds", "/workflows/w1")]
    surfaces += [getattr(labels.DEFAULT, f) for f in vars(labels.DEFAULT)]
    for text in surfaces:
        visible = re.sub(r"<script.*?</script>|<style.*?</style>", "", text, flags=re.S)
        assert not re.search(r"\b(you|your|yours)\b", visible, re.I), visible[:200]


# ── Audit ────────────────────────────────────────────────────────────────────

def _policy_record(request_id="req-77", metric="cost", threshold=5.0,
                   trigger=7.32, window=10, action="pause"):
    from boundflow.control_plane import PolicyActionRecord
    return PolicyActionRecord(
        workflow_id="w1", request_id=request_id, metric=metric, threshold=threshold,
        window=window, tool="", action=action, target_version=0, cooldown_seconds=0,
        trigger_value=trigger, previous_version=1, previous_state="active",
        actor="system", occurred_at=NOW,
    )


def test_a_policy_pause_says_what_crossed_and_which_run_caused_it():
    """Naming the cause without the reason left the operator to go find the rule."""
    paused = _wf("w1", lifecycle=LifecycleState.BLOCKED, state=WorkflowState.PAUSED)
    paused.last_policy_decision_request_id = "req-77"
    body = views.status_callout(paused, policy=_policy_record())

    assert "cost reached 7.32" in body
    assert "threshold of 5" in body
    assert "over 10 runs" in body
    assert "req-77" in body                     # the run that triggered it


def test_the_callout_still_renders_without_its_audit_record():
    """The policy audit read is allowed to fail; the page must not."""
    paused = _wf("w1", lifecycle=LifecycleState.BLOCKED, state=WorkflowState.PAUSED)
    paused.last_policy_decision_request_id = "req-77"
    body = views.status_callout(paused, policy=None)
    assert "lifecycle policy" in body
    assert "req-77" in body


def test_the_detail_page_shows_the_audit_log():
    """Every decision is recorded server-side; a console that shows state but not
    how it got there sends the operator to the CLI for the usual question."""
    from boundflow.control_plane import ApprovalDecision, ApprovalAuditRecord

    approval = ApprovalAuditRecord(
        workflow_id="w1", request_id="req-1", approval_id="ap-1",
        decision=ApprovalDecision.REJECTED, opened_at=NOW, decided_at=NOW,
        actor="ops@example.com", occurred_at=NOW,
        justification="refund $5,000", reason="over the limit")
    c, _ = client([_wf("w1")], audit=[approval, _policy_record()])
    body = c.get("/workflows/w1").text

    assert "Audit" in body
    assert "ops@example.com" in body
    assert "refund $5,000" in body and "over the limit" in body
    assert "rejected" in body
    assert "workflow policy" in body            # the policy firing, same table
    assert "req-1" in body and "req-77" in body  # each tied to its run


def test_the_detail_page_survives_the_audit_read_failing():
    c, cp = client([_wf("w1")])

    async def boom(wid=""):
        raise RuntimeError("audit unavailable")

    cp.get_audit_log = boom
    cp.get_workflow_policy_audit = boom
    r = c.get("/workflows/w1")
    assert r.status_code == 200
    assert "Nothing recorded yet." in r.text


# ── Nothing the control plane returns gets dropped ───────────────────────────

def test_every_field_the_control_plane_returns_reaches_the_page():
    """These types exist field by field for a reason; a renderer that picks by hand
    silently drops whatever nobody remembered. This is the guard against that."""
    import dataclasses
    from boundflow.control_plane import (
        ApprovalDecision, ApprovalAuditRecord, InputAuditRecord, InputDecision,
        InvokeMode, WorkflowConfig,
    )

    approval = PendingApproval("ap-1", "why", {"amount": 500}, NOW, NOW)
    gate_input = PendingInput("in-1", "which?", {"choices": 2}, NOW, NOW)
    w = _wf("w1", lifecycle=LifecycleState.AWAITING_APPROVAL, approval=approval,
            pending_input=gate_input,
            suspension=Suspension("s1", "reason", True, NOW, NOW))
    w.config = WorkflowConfig(1, 300, 60, True, InvokeMode.QUEUE, 5, True)
    w.deletion_requested_at = None

    audit = [
        ApprovalAuditRecord("w1", "req-1", "ap-9", ApprovalDecision.REJECTED, NOW, NOW,
                            "ops", NOW, "justify", "because"),
        InputAuditRecord("w1", "req-2", "in-9", InputDecision.ANSWERED, NOW, NOW,
                         "ops", NOW, {"region": "us"}, "prompt?"),
        _policy_record(),
    ]
    run = Run("req-3", "invoke", RunStatus.FAILED, RunOutcome.OPERATION_TIMEOUT,
              "too slow", NOW, NOW)

    c, _ = client([w], audit=audit)
    page = c.get("/workflows/w1").text + views.runs_table([run])

    def field_values(obj):
        for f in dataclasses.fields(obj):
            v = getattr(obj, f.name)
            if v is None or v == "" or v == {} or isinstance(v, bool):
                continue          # nothing to look for, or rendered as yes/no
            yield f.name, v

    for obj in (w, w.config, w.suspension, approval, gate_input, run, *audit):
        for name, value in field_values(obj):
            if dataclasses.is_dataclass(value) or isinstance(value, (dict, list)):
                continue          # rendered structurally; covered by its own case
            rendered = views.esc(value) if hasattr(views, "esc") else str(value)
            assert rendered in page or str(value) in page, \
                f"{type(obj).__name__}.{name} = {value!r} never reaches the page"


def test_abandon_queued_is_offered_and_mirrors_the_rpc():
    """The CLI's abandon-queued had no console counterpart at all."""
    c, cp = client([_wf("w1")])
    captured = []

    async def go(wid, request_ids=None, all=False):
        captured.append((wid, request_ids, all))
        return request_ids or []

    cp.abandon_queued_requests = go

    c.post("/workflows/w1/abandon", data={"request_ids": "r1, r2"},
           follow_redirects=False)
    assert captured == [("w1", ["r1", "r2"], False)]

    captured.clear()
    c.post("/workflows/w1/abandon", data={"all": "1"}, follow_redirects=False)
    assert captured == [("w1", None, True)]


def test_abandon_requires_exactly_one_of_ids_or_all():
    """Same rule the CLI enforces; the console shouldn't send an ambiguous request."""
    c, cp = client([_wf("w1")])
    cp.abandon_queued_requests = lambda *a, **k: pytest.fail("should not be called")

    both = c.post("/workflows/w1/abandon", data={"request_ids": "r1", "all": "1"},
                  follow_redirects=False)
    assert "not+both" in both.headers["location"].replace("%20", "+")

    neither = c.post("/workflows/w1/abandon", data={}, follow_redirects=False)
    assert "error=" in neither.headers["location"]


def test_suspend_can_retarget_an_existing_hold():
    """suspend_workflow takes suspension_id to retarget; the form never offered it."""
    c, cp = client([_wf("w1")])
    captured = []

    async def go(wid, reason="", stop_current_run=False, suspension_id=""):
        captured.append(suspension_id)

    cp.suspend_workflow = go
    c.post("/workflows/w1/suspend",
           data={"reason": "r", "suspension_id": "sus-existing"},
           follow_redirects=False)
    assert captured == ["sus-existing"]


# ── Deleted view ─────────────────────────────────────────────────────────────

def _tombstone(wid="w-gone"):
    w = _wf(wid, lifecycle=LifecycleState.DELETED, state=WorkflowState.DISABLED)
    w.deletion_requested_at = NOW
    return w


def test_the_fleet_excludes_tombstones_and_counts_them_separately():
    """Deletion is soft plus a periodic purge, so deleted workflows keep being
    returned for a while — enough of them to swamp the fleet they're mixed into."""
    c, _ = client([_wf("w-live"), _tombstone("w-gone")])
    home = c.get("/").text

    assert "w-live" in home
    assert "w-gone" not in home
    assert "Fleet<b>1</b>" in home
    assert "Deleted<b>1</b>" in home


def test_the_deleted_view_lists_them_with_when_deletion_was_requested():
    c, _ = client([_wf("w-live"), _tombstone("w-gone")])
    body = c.get("/deleted").text

    assert "w-gone" in body
    assert "w-live" not in body
    assert "deletion requested" in body


def test_the_deleted_view_is_empty_when_nothing_is_deleted():
    c, _ = client([_wf("w-live")])
    assert "Nothing deleted." in c.get("/deleted").text


def test_a_requested_but_unfinalized_deletion_stays_in_the_fleet_and_says_so():
    """It is still draining and may still be running, so it isn't a tombstone yet —
    but the console said nothing about it at all before."""
    draining = _wf("w1")
    draining.deletion_requested_at = NOW

    c, _ = client([draining])
    assert "w1" in c.get("/").text              # still the fleet's problem
    assert "w1" not in c.get("/deleted").text   # not finalized

    callout = views.status_callout(draining)
    assert "Deletion requested." in callout
    assert "Waiting for anything in flight" in callout


# ── Fleet sorting and the tenant filter ──────────────────────────────────────

def _fleet(*specs):
    """(id, type, tenant) triples."""
    out = []
    for wid, wtype, tid in specs:
        w = _wf(wid, wtype=wtype)
        w.tenant_id = tid
        out.append(w)
    return out


def test_the_fleet_can_be_filtered_to_one_tenant():
    c, _ = client(_fleet(("w-a", "alpha", "tenant-1"), ("w-b", "beta", "tenant-2")))
    body = c.get("/?tenant=tenant-1").text

    assert "w-a" in body
    assert "w-b" not in body
    assert "Filtered to tenant" in body
    assert "show all" in body                   # and a way back out


def test_the_fleet_count_reflects_the_tenant_filter():
    c, _ = client(_fleet(("w-a", "alpha", "tenant-1"), ("w-b", "beta", "tenant-2")))
    assert "Fleet (1)" in c.get("/?tenant=tenant-1").text
    assert "Fleet (2)" in c.get("/").text


def test_a_tenant_is_a_link_that_filters_to_it():
    c, _ = client(_fleet(("w-a", "alpha", "tenant-1")))
    assert "?tenant=tenant-1" in c.get("/").text


def test_the_fleet_sorts_by_a_column_and_flips_on_a_second_click():
    c, _ = client(_fleet(("w-a", "zeta", "t1"), ("w-b", "alpha", "t1")))

    asc = c.get("/?sort=type").text
    assert asc.index("alpha") < asc.index("zeta")

    desc = c.get("/?sort=type&desc=1").text
    assert desc.index("zeta") < desc.index("alpha")


def test_an_unknown_sort_key_leaves_the_order_alone():
    """A hand-typed URL shouldn't take the page down."""
    workflows = _fleet(("w-a", "zeta", "t1"), ("w-b", "alpha", "t1"))
    assert views.sort_workflows(workflows, "not-a-column") == workflows

    c, _ = client(workflows)
    assert c.get("/?sort=not-a-column").status_code == 200


def test_the_poll_keeps_the_sort_and_the_filter():
    """The table reloads every few seconds; a sort it dropped would be worse than
    no sort at all."""
    c, _ = client(_fleet(("w-a", "alpha", "tenant-1"), ("w-b", "beta", "tenant-2")))
    body = c.get("/?sort=type&desc=1&tenant=tenant-1").text
    assert "sort=type" in body and "desc=1" in body and "tenant=tenant-1" in body

    fragment = c.get("/fragment/fleet?sort=type&desc=1&tenant=tenant-1").text
    assert "w-a" in fragment and "w-b" not in fragment


def test_cooldown_until_reaches_the_page_and_is_named_in_the_callout():
    """The column was loaded on every read and dropped at the proto boundary, so a
    cooldown could be explained but not timed."""
    from datetime import timedelta

    cooling = _wf("w1", lifecycle=LifecycleState.BLOCKED, state=WorkflowState.COOLDOWN)
    cooling.last_policy_decision_request_id = "req-9"
    cooling.cooldown_until = NOW + timedelta(minutes=30)

    assert "Scheduling resumes" in views.status_callout(cooling)

    c, _ = client([cooling])
    assert "cooldown until" in c.get("/workflows/w1").text.lower()
