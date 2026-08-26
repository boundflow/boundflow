"""boundflow audit — audit log CLI tests.

All tests are control-plane-only (no worker required). A fresh workflow has
empty audit records — that's the observable state we assert on here. Tests that
need populated audit records are in the SDK integration suite (test_approval_audit.py,
test_policy_audit.py, etc.) since those require a live worker run.
"""
from __future__ import annotations

import re
from types import SimpleNamespace

from boundflow.cli.commands import audit

from .conftest import make_tenant, make_workflow, run


def test_audit_log_returns_empty_list_for_new_workflow(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "aud-log")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    results = run(runner, boundflow_api_key, ["audit", "log", wf_id])
    assert isinstance(results, list)
    assert results == []


def test_audit_log_without_workflow_id_returns_list(runner, boundflow_api_key):
    results = run(runner, boundflow_api_key, ["audit", "log"])
    assert isinstance(results, list)


def test_audit_approvals_returns_empty_for_new_workflow(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "aud-app")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    results = run(runner, boundflow_api_key, ["audit", "approvals", wf_id])
    assert isinstance(results, list)
    assert results == []


def test_audit_inputs_returns_empty_for_new_workflow(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "audit-inputs")
    workflow_id = make_workflow(runner, boundflow_api_key, tenant_id)
    data = run(runner, boundflow_api_key, ["audit", "inputs", workflow_id])
    assert data == []


def test_audit_workflow_policy_returns_empty_for_new_workflow(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "aud-wfpol")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    results = run(runner, boundflow_api_key, ["audit", "workflow", wf_id])
    assert isinstance(results, list)
    assert results == []


def test_audit_agent_policy_returns_empty_for_new_workflow(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "aud-agpol")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    results = run(runner, boundflow_api_key, ["audit", "agent", wf_id, "my-agent"])
    assert isinstance(results, list)
    assert results == []


def test_audit_log_scoped_to_workflow(runner, boundflow_api_key):
    """Workflow-scoped log returns a list (empty for a new workflow, never an error)."""
    tenant_id = make_tenant(runner, boundflow_api_key, "aud-scope")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    results = run(runner, boundflow_api_key, ["audit", "log", wf_id])
    assert isinstance(results, list)


def test_audit_log_table_aligns_heterogeneous_records(runner, monkeypatch):
    """The real non-JSON CLI path projects mixed audit rows by column key."""
    records = [
        SimpleNamespace(approval_id="approval-1", actor="alice"),
        SimpleNamespace(metric="failures", action="pause", actor="system"),
    ]
    monkeypatch.setattr(audit, "cp_call", lambda _call: records)

    result = run(runner, "test-key", ["audit", "log"], json_out=False)

    table_lines = [line.strip() for line in result.stdout.splitlines()]
    cells = [
        [cell.strip() for cell in re.split("[│┃]", line)[1:-1]]
        for line in table_lines
        if line[:1] in "│┃" and line[-1:] in "│┃"
    ]
    assert ["approval_id", "actor", "metric", "action"] in cells
    assert ["approval-1", "alice", "", ""] in cells
    assert ["", "system", "failures", "pause"] in cells
