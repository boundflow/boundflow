"""boundflow tenant — tenant group and tenant management CLI tests.

Control-plane only (no worker needed). Each test creates isolated resources.
"""
from __future__ import annotations

from .conftest import make_tenant, run, run_expect_fail


def test_create_tenant_returns_id_and_name(runner, boundflow_api_key):
    data = run(runner, boundflow_api_key, ["tenant", "create", "acme-cli-test"])
    assert "id" in data and data["id"]
    assert data["name"] == "acme-cli-test"
    assert "tenant_group_id" in data and data["tenant_group_id"]


def test_create_tenant_ids_are_unique(runner, boundflow_api_key):
    a = run(runner, boundflow_api_key, ["tenant", "create", "dup-a"])
    b = run(runner, boundflow_api_key, ["tenant", "create", "dup-b"])
    assert a["id"] != b["id"]


def test_get_tenant_returns_correct_fields(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "get")
    data = run(runner, boundflow_api_key, ["tenant", "get", tenant_id])
    assert data["id"] == tenant_id
    assert "name" in data
    assert "tenant_group_id" in data


def test_get_tenant_group_matches_tenant(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "grp")
    tenant = run(runner, boundflow_api_key, ["tenant", "get", tenant_id])
    group_id = tenant["tenant_group_id"]

    group = run(runner, boundflow_api_key, ["tenant", "group", "get", group_id])
    assert group["id"] == group_id
    assert "name" in group


def test_delete_tenant_succeeds(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "del")
    result = run(runner, boundflow_api_key, ["tenant", "delete", tenant_id, "--yes"])
    assert result["status"] == "ok"


def test_delete_tenant_makes_it_unreachable(runner, boundflow_api_key):
    tenant_id = make_tenant(runner, boundflow_api_key, "del-check")
    run(runner, boundflow_api_key, ["tenant", "delete", tenant_id, "--yes"])
    run_expect_fail(runner, boundflow_api_key, ["tenant", "get", tenant_id])


def test_get_nonexistent_tenant_fails(runner, boundflow_api_key):
    run_expect_fail(
        runner, boundflow_api_key,
        ["tenant", "get", "00000000-0000-0000-0000-000000000000"],
    )


def test_get_nonexistent_tenant_group_fails(runner, boundflow_api_key):
    run_expect_fail(
        runner, boundflow_api_key,
        ["tenant", "group", "get", "00000000-0000-0000-0000-000000000000"],
    )
