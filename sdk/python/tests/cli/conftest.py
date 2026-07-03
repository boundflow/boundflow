"""Shared fixtures and helpers for boundflow CLI integration tests.

All tests run against a live stack (BOUNDFLOW_API_KEY required). Each test
creates isolated resources so tests are fully independent and can run in any
order. The CliRunner is synchronous — asyncio.run() fires internally per command.
"""
from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest
from typer.testing import CliRunner

from boundflow.cli import app

SERVER_ADDRESS = "http://localhost:50051"


@pytest.fixture
def boundflow_api_key():
    key = os.environ.get("BOUNDFLOW_API_KEY")
    if not key:
        pytest.skip("BOUNDFLOW_API_KEY not set")
    return key


@pytest.fixture
def runner():
    return CliRunner()


# ── Invocation helpers ───────────────────────────────────────────────────────


def run(runner: CliRunner, api_key: str, args: list, *, json_out: bool = True):
    """Invoke a boundflow command; assert success; return parsed JSON or raw result.

    Passes --server and --api-key on every call so tests are independent of
    shell environment. Passes --json for machine-readable output by default.
    """
    base = ["--server", SERVER_ADDRESS, "--api-key", api_key]
    if json_out:
        base = ["--json"] + base
    result = runner.invoke(app, base + list(args))
    if result.exit_code != 0:
        raise AssertionError(
            f"boundflow {' '.join(args)} failed (exit {result.exit_code}):\n"
            f"{result.stdout}"
            + (f"\n{result.exception}" if result.exception else "")
        )
    if json_out:
        return json.loads(result.stdout)
    return result


def run_expect_fail(runner: CliRunner, api_key: str, args: list):
    """Invoke a boundflow command and assert it exits non-zero."""
    base = ["--json", "--server", SERVER_ADDRESS, "--api-key", api_key]
    result = runner.invoke(app, base + list(args), catch_exceptions=True)
    assert result.exit_code != 0, (
        f"Expected failure but exit_code=0:\n{result.stdout}"
    )
    return result


# ── Resource factories ───────────────────────────────────────────────────────


def make_tenant(runner: CliRunner, api_key: str, prefix: str = "cli-t") -> str:
    """Create an isolated tenant and return its ID."""
    uid = uuid.uuid4().hex[:8]
    data = run(runner, api_key, ["tenant", "create", f"{prefix}-{uid}"])
    return data["id"]


def make_workflow(
    runner: CliRunner,
    api_key: str,
    tenant_id: str,
    workflow_type: str = "cli-wf",
    *,
    version: int = 1,
) -> str:
    """Create a workflow and return its ID."""
    uid = uuid.uuid4().hex[:6]
    data = run(
        runner,
        api_key,
        ["workflow", "create", f"{workflow_type}-{uid}", tenant_id, "--version", str(version)],
    )
    return data["id"]


def write_json_file(tmp_path: Path, name: str, content) -> Path:
    """Write JSON to a temp file without BOM (avoids the PowerShell utf-8-sig issue)."""
    p = tmp_path / name
    p.write_bytes(json.dumps(content).encode("utf-8"))
    return p
