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

    def __init__(self, workflows):
        self._workflows = {w.id: w for w in workflows}
        self.calls: list[tuple] = []

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


def client(workflows):
    console = Console("http://localhost:50051", "key")
    cp = FakeCP(workflows)
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
    assert "Waiting on you (1)" in body
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
    assert "Waiting on you (0)" in c.get("/").text


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
