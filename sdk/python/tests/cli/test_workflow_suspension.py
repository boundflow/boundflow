"""boundflow workflow suspend/resume/abandon-queued — CLI tests.

No worker is needed: an idle workflow has nothing to drain, so its hold finalizes on its
own and can be resumed. Cases that need a run in flight (retargeting, cutting) live in the
SDK integration suite, since only a real worker can hold one open.
"""
from __future__ import annotations

import time

from .conftest import make_tenant, make_workflow, run, run_expect_fail


def _held(runner, api_key, workflow_id):
    """Poll until the hold finalizes — until then there is nothing to resume."""
    for _ in range(60):
        wf = run(runner, api_key, ["workflow", "get", workflow_id])
        s = wf.get("suspension")
        if s and s.get("finalized_at"):
            return wf
        time.sleep(1)
    raise AssertionError(f"suspension never finalized: {wf.get('suspension')}")


# ── Suspend ──────────────────────────────────────────────────────────────────


def test_suspend_returns_id_and_shows_on_the_workflow(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "susp")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id], json_out=False)

    data = run(runner, boundflow_api_key,
               ["workflow", "suspend", wf_id, "--reason", "operator hold"])
    assert data["suspension_id"]

    wf = run(runner, boundflow_api_key, ["workflow", "get", wf_id])
    assert wf["workflow_state"] == "suspended"
    assert wf["suspension"]["suspension_id"] == data["suspension_id"]
    assert wf["suspension"]["reason"] == "operator hold"
    assert wf["suspension"]["stop_current"] is False


def test_suspend_stop_current_run_flag_is_recorded(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "susp-cut")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id], json_out=False)

    run(runner, boundflow_api_key,
        ["workflow", "suspend", wf_id, "--stop-current-run"])
    wf = run(runner, boundflow_api_key, ["workflow", "get", wf_id])
    assert wf["suspension"]["stop_current"] is True


def test_suspend_twice_without_an_id_is_refused(runner, boundflow_api_key):
    """A second hold is a mistake worth reporting, not a silent no-op."""
    tenant_id = make_tenant(runner, boundflow_api_key, "susp-twice")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id], json_out=False)

    run(runner, boundflow_api_key, ["workflow", "suspend", wf_id])
    run_expect_fail(runner, boundflow_api_key, ["workflow", "suspend", wf_id])


# ── Resume ───────────────────────────────────────────────────────────────────


def test_resume_releases_the_hold(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "resume")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id], json_out=False)

    sid = run(runner, boundflow_api_key, ["workflow", "suspend", wf_id])["suspension_id"]
    _held(runner, boundflow_api_key, wf_id)

    run(runner, boundflow_api_key, ["workflow", "resume", wf_id, sid], json_out=False)
    wf = run(runner, boundflow_api_key, ["workflow", "get", wf_id])
    assert wf["workflow_state"] == "active"
    assert not wf.get("suspension")


def test_resume_with_a_wrong_id_is_refused(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "resume-bad")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id], json_out=False)
    run(runner, boundflow_api_key, ["workflow", "suspend", wf_id])
    _held(runner, boundflow_api_key, wf_id)

    run_expect_fail(runner, boundflow_api_key,
                    ["workflow", "resume", wf_id, "not-the-right-id"])


# ── Abandon queued ───────────────────────────────────────────────────────────


def test_abandon_queued_requires_exactly_one_of_all_or_ids(runner, boundflow_api_key):
    """Dropping a backlog can't be undone, so a forgotten flag must not do it."""
    tenant_id = make_tenant(runner, boundflow_api_key, "abandon-guard")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    run_expect_fail(runner, boundflow_api_key, ["workflow", "abandon-queued", wf_id])
    run_expect_fail(runner, boundflow_api_key,
                    ["workflow", "abandon-queued", wf_id, "--all", "--request-id", "x"])


def test_abandon_queued_leaves_a_run_that_is_no_longer_queued(runner, boundflow_api_key):
    """Only unscheduled and held runs are eligible; one already scheduled is untouched."""
    tenant_id = make_tenant(runner, boundflow_api_key, "abandon-sched")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id], json_out=False)
    run(runner, boundflow_api_key, ["workflow", "invoke", wf_id])

    data = run(runner, boundflow_api_key, ["workflow", "abandon-queued", wf_id, "--all"])
    assert data["abandoned"] == []
