"""boundflow workflow — workflow lifecycle CLI tests.

Invoke tests do not require a worker: the server accepts the request and returns
a request_id immediately; with no worker to pick it up the run sits in SCHEDULED.
Tests that need a completed run are in the SDK integration suite
(test_approval_gate.py, etc.).
"""
from __future__ import annotations

from .conftest import make_tenant, make_workflow, run, run_expect_fail


# ── Create ───────────────────────────────────────────────────────────────────


def test_create_workflow_returns_id_and_config(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-create")
    data = run(runner, boundflow_api_key,
               ["workflow", "create", "order-triage", tenant_id, "--version", "3"])
    assert "id" in data and data["id"]
    assert data["tenant_id"] == tenant_id
    assert data["config"]["version"] == 3


def test_create_workflow_timeout_and_repeat_flags(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-flags")
    data = run(runner, boundflow_api_key,
               ["workflow", "create", "scheduled-job", tenant_id,
                "--timeout", "120", "--repeat", "300"])
    assert data["config"]["invoke_timeout_seconds"] == 120
    assert data["config"]["repeat_every_seconds"] == 300


def test_create_workflow_no_triggerable_flag(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-notrig")
    data = run(runner, boundflow_api_key,
               ["workflow", "create", "background-job", tenant_id, "--no-triggerable"])
    assert data["config"]["triggerable"] is False


# ── Activate ─────────────────────────────────────────────────────────────────


def test_activate_workflow_succeeds(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-act")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    result = run(runner, boundflow_api_key, ["workflow", "activate", wf_id])
    assert result["status"] == "ok"


# ── Get ──────────────────────────────────────────────────────────────────────


def test_get_workflow_shows_lifecycle_and_workflow_state(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-get")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    data = run(runner, boundflow_api_key, ["workflow", "get", wf_id])
    assert data["id"] == wf_id
    assert "version" in data
    assert "lifecycle_state" in data
    assert "workflow_state" in data


def test_get_workflow_reflects_activate(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-get-act")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    before = run(runner, boundflow_api_key, ["workflow", "get", wf_id])
    assert before["workflow_state"] == "paused"

    run(runner, boundflow_api_key, ["workflow", "activate", wf_id])

    after = run(runner, boundflow_api_key, ["workflow", "get", wf_id])
    assert after["workflow_state"] == "active"


def test_get_nonexistent_workflow_fails(runner, boundflow_api_key):
    run_expect_fail(
        runner, boundflow_api_key,
        ["workflow", "get", "00000000-0000-0000-0000-000000000000"],
    )


# ── List ─────────────────────────────────────────────────────────────────────


def test_list_workflows_includes_created(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-list")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    results = run(runner, boundflow_api_key, ["workflow", "list"])
    ids = {w["id"] for w in results}
    assert wf_id in ids


def test_list_workflows_shows_correct_fields(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-list-fields")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id, "field-check-wf", version=2)

    by_id = {w["id"]: w for w in run(runner, boundflow_api_key, ["workflow", "list"])}
    assert wf_id in by_id
    wf = by_id[wf_id]
    assert wf["version"] == 2
    assert wf["tenant_id"] == tenant_id
    assert wf["lifecycle_state"] == "active"
    assert wf["workflow_state"] == "paused"


def test_list_workflows_reflects_activate_state(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-list-state")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    before = {w["id"]: w for w in run(runner, boundflow_api_key, ["workflow", "list"])}
    assert before[wf_id]["workflow_state"] == "paused"

    run(runner, boundflow_api_key, ["workflow", "activate", wf_id])

    after = {w["id"]: w for w in run(runner, boundflow_api_key, ["workflow", "list"])}
    assert after[wf_id]["workflow_state"] == "active"


def test_list_workflows_spans_multiple_tenants(runner, boundflow_api_key):
    t1 = make_tenant(runner, boundflow_api_key, "wf-span-a")
    t2 = make_tenant(runner, boundflow_api_key, "wf-span-b")
    wf1 = make_workflow(runner, boundflow_api_key, t1, "type-a")
    wf2 = make_workflow(runner, boundflow_api_key, t2, "type-b")

    ids = {w["id"] for w in run(runner, boundflow_api_key, ["workflow", "list"])}
    assert {wf1, wf2} <= ids


# ── Invoke ───────────────────────────────────────────────────────────────────


def test_invoke_returns_request_id(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-inv")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id])

    data = run(runner, boundflow_api_key, ["workflow", "invoke", wf_id])
    assert "request_id" in data and data["request_id"]


def test_invoke_transitions_to_scheduled_state(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-inv-state")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id])
    run(runner, boundflow_api_key, ["workflow", "invoke", wf_id])

    # With no worker connected, the run waits for pickup: 'scheduled'. It only
    # becomes 'invoking' once a worker actually dispatches the operation.
    state = run(runner, boundflow_api_key, ["workflow", "get", wf_id])
    assert state["lifecycle_state"] == "scheduled"


# ── Delete ───────────────────────────────────────────────────────────────────


def test_delete_workflow_removes_it(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-del")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    result = run(runner, boundflow_api_key, ["workflow", "delete", wf_id, "--yes"])
    assert result["status"] == "ok"

    run_expect_fail(runner, boundflow_api_key, ["workflow", "get", wf_id])


# ── Runs ─────────────────────────────────────────────────────────────────────


def test_runs_lists_the_invocation(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-runs")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id])
    request_id = run(runner, boundflow_api_key, ["workflow", "invoke", wf_id])["request_id"]

    runs = run(runner, boundflow_api_key, ["workflow", "runs", wf_id])
    assert any(r["request_id"] == request_id for r in runs)


def test_request_returns_run_info(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-req")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id])
    request_id = run(runner, boundflow_api_key, ["workflow", "invoke", wf_id])["request_id"]

    info = run(runner, boundflow_api_key, ["workflow", "request", request_id])
    assert info["request_id"] == request_id
    assert info["workflow_id"] == wf_id
    assert info["status"], "expected a run status"


def test_resolve_uninterrupted_workflow_fails(runner, boundflow_api_key):
    # A workflow that isn't interrupted can't be resolved → non-zero exit.
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-resolve")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, ["workflow", "activate", wf_id])

    run_expect_fail(runner, boundflow_api_key, ["workflow", "resolve", wf_id, "some-request-id"])


# ── JSON output ───────────────────────────────────────────────────────────────


def test_json_output_is_valid_json(runner, boundflow_api_key):
    """--json flag must emit parseable JSON for both list and record responses."""
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-json")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    # List → JSON array
    list_data = run(runner, boundflow_api_key, ["workflow", "list"])
    assert isinstance(list_data, list)

    # Record → JSON object
    get_data = run(runner, boundflow_api_key, ["workflow", "get", wf_id])
    assert isinstance(get_data, dict)
    assert get_data["id"] == wf_id


def test_json_list_contains_no_enum_class_prefix(runner, boundflow_api_key):
    """Enum values must be plain strings like 'active', not 'LifecycleState.active'."""
    tenant_id = make_tenant(runner, boundflow_api_key, "wf-enum")
    make_workflow(runner, boundflow_api_key, tenant_id)

    workflows = run(runner, boundflow_api_key, ["workflow", "list"])
    assert workflows, "expected at least one workflow"
    wf = next(w for w in workflows if w["tenant_id"] == tenant_id)
    assert "." not in wf["lifecycle_state"], f"enum leak: {wf['lifecycle_state']!r}"
    assert "." not in wf["workflow_state"], f"enum leak: {wf['workflow_state']!r}"
