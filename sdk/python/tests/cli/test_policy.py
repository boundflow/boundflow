"""boundflow policy — runtime and lifecycle policy CLI tests.

Tests both the --flags path and the --file path (JSON without BOM).
"""
from __future__ import annotations

from .conftest import make_tenant, make_workflow, run, write_json_file


# ── Runtime policy ────────────────────────────────────────────────────────────


def test_runtime_policy_via_flags(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-rt-flags")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    result = run(runner, boundflow_api_key, [
        "policy", "runtime", wf_id, "my-agent",
        "--max-llm-calls", "5",
        "--max-cost-usd", "0.25",
        "--max-tokens-per-call", "4096",
    ])
    assert result["status"] == "ok"


def test_runtime_policy_tool_limit_flag(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-rt-tool")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    result = run(runner, boundflow_api_key, [
        "policy", "runtime", wf_id, "my-agent",
        "--tool-limit", "search:3",
        "--tool-limit", "write:1",
    ])
    assert result["status"] == "ok"


def test_runtime_policy_via_file(runner, boundflow_api_key, tmp_path):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-rt-file")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    policy_file = write_json_file(tmp_path, "runtime.json", {
        "max_llm_calls": 3,
        "max_cost_usd": 0.10,
        "max_tokens_per_call": 2048,
        "tool_call_limits": [{"tool": "search", "max_calls": 2}],
    })

    result = run(runner, boundflow_api_key, [
        "policy", "runtime", wf_id, "my-agent", "--file", str(policy_file),
    ])
    assert result["status"] == "ok"


def test_runtime_policy_file_and_flags_mutual_exclusion(runner, boundflow_api_key, tmp_path):
    """Passing both --file and a flag should still succeed (file takes precedence)."""
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-rt-both")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    policy_file = write_json_file(tmp_path, "runtime.json", {"max_llm_calls": 2})
    result = run(runner, boundflow_api_key, [
        "policy", "runtime", wf_id, "my-agent",
        "--file", str(policy_file),
        "--max-llm-calls", "99",  # ignored when --file is present
    ])
    assert result["status"] == "ok"


# ── Agent lifecycle policy ────────────────────────────────────────────────────


def test_agent_lifecycle_policy_via_file(runner, boundflow_api_key, tmp_path):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-lc-agent")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    rules_file = write_json_file(tmp_path, "agent_rules.json", [
        {
            "metric": "cost_usd",
            "op": "greater_than",
            "threshold": 0.50,
            "window": 1,
            "action": {"field": "model", "value": "claude-haiku-4-5"},
        }
    ])

    result = run(runner, boundflow_api_key, [
        "policy", "lifecycle", "set-agent", wf_id, "my-agent",
        "--file", str(rules_file),
    ])
    assert result["status"] == "ok"


def test_agent_lifecycle_policy_multiple_rules(runner, boundflow_api_key, tmp_path):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-lc-multi")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    rules_file = write_json_file(tmp_path, "multi_rules.json", [
        {
            "metric": "llm_calls",
            "op": "greater_than",
            "threshold": 10,
            "window": 1,
            "action": {"field": "max_llm_calls", "value": 5},
        },
        {
            "metric": "cost_usd",
            "op": "greater_than",
            "threshold": 1.0,
            "window": 1,
            "action": {"field": "model", "value": "claude-haiku-4-5"},
        },
    ])

    result = run(runner, boundflow_api_key, [
        "policy", "lifecycle", "set-agent", wf_id, "my-agent",
        "--file", str(rules_file),
    ])
    assert result["status"] == "ok"


# ── Workflow lifecycle policy ─────────────────────────────────────────────────


def test_workflow_lifecycle_policy_cooldown_via_file(runner, boundflow_api_key, tmp_path):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-wf-cd")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    rules_file = write_json_file(tmp_path, "wf_rules.json", [
        {
            "metric": "num_failures",
            "threshold": 1,
            "action": {"kind": "cooldown", "window": 1, "seconds": 10},
        }
    ])

    result = run(runner, boundflow_api_key, [
        "policy", "lifecycle", "set-workflow", wf_id,
        "--file", str(rules_file),
    ])
    assert result["status"] == "ok"


def test_workflow_lifecycle_policy_set_version_via_file(runner, boundflow_api_key, tmp_path):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-wf-ver")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    rules_file = write_json_file(tmp_path, "ver_rules.json", [
        {
            "metric": "num_llm_calls",
            "threshold": 5,
            "action": {"kind": "set_version", "target": 2},
        }
    ])

    result = run(runner, boundflow_api_key, [
        "policy", "lifecycle", "set-workflow", wf_id,
        "--file", str(rules_file),
    ])
    assert result["status"] == "ok"


# ── Policy getters (read the armed policy back) ───────────────────────────────


def test_get_runtime_policy_reads_back(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-get-rt")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    run(runner, boundflow_api_key, [
        "policy", "runtime", wf_id, "analyst",
        "--max-cost-usd", "0.05", "--max-llm-calls", "8",
    ])
    got = run(runner, boundflow_api_key, ["policy", "get-runtime", wf_id, "analyst"])
    assert got["max_cost_usd"] == 0.05
    assert got["max_llm_calls"] == 8


def test_get_runtime_policy_empty_when_unset(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-get-rt-empty")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    got = run(runner, boundflow_api_key, ["policy", "get-runtime", wf_id, "nobody"])
    assert got == {}


def test_get_workflow_lifecycle_policy_reads_back(runner, boundflow_api_key, tmp_path):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-get-wf")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    rules_file = write_json_file(tmp_path, "wf.json", [
        {"metric": "cost", "threshold": 5.0, "action": {"kind": "set_version", "target": 1}},
    ])
    run(runner, boundflow_api_key, ["policy", "lifecycle", "set-workflow", wf_id, "--file", str(rules_file)])
    rules = run(runner, boundflow_api_key, ["policy", "lifecycle", "get-workflow", wf_id])
    assert len(rules) == 1
    assert rules[0]["metric"] == "cost"
    assert rules[0]["action"]["kind"] == "set_version"
    assert rules[0]["action"]["target"] == 1


def test_get_agent_lifecycle_policy_empty_when_unset(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-get-agent-empty")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)
    got = run(runner, boundflow_api_key, ["policy", "lifecycle", "get-agent", wf_id, "nobody"])
    assert got == {}


def test_workflow_lifecycle_policy_pause_via_file(runner, boundflow_api_key, tmp_path):
    tenant_id = make_tenant(runner, boundflow_api_key, "pol-wf-pause")
    wf_id = make_workflow(runner, boundflow_api_key, tenant_id)

    rules_file = write_json_file(tmp_path, "pause_rules.json", [
        {
            "metric": "approval_rejections",
            "threshold": 1,
            "action": {"kind": "pause", "window": 1},
        }
    ])

    result = run(runner, boundflow_api_key, [
        "policy", "lifecycle", "set-workflow", wf_id,
        "--file", str(rules_file),
    ])
    assert result["status"] == "ok"
