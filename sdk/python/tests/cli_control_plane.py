"""A `cp`-shaped adapter that routes control-plane calls through the `boundflow` CLI.

Used by the e2e suite when run with `--cp-via-cli`: the same test scenarios exercise
the CLI (arg parsing, --json output, the whole command path) against a live backend,
instead of the SDK directly. Only the control-plane operations route through the CLI;
the worker stays in-process (it runs your agent handlers).

Any method without a CLI command falls through to the real SDK client (see __getattr__),
so structured-policy ops (set_*_lifecycle_policy) and anything not yet wired keep working.
"""
from __future__ import annotations

import asyncio
import json
import sys
from types import SimpleNamespace

from boundflow.control_plane import LifecycleState, WorkflowState


class CliError(RuntimeError):
    """A `boundflow` CLI invocation exited non-zero."""


class CliControlPlane:
    def __init__(self, server: str, api_key: str, sdk_fallback):
        self._server = server
        self._api_key = api_key
        self._sdk = sdk_fallback  # real ControlPlaneClient for ops without a CLI command

    # ── subprocess plumbing ───────────────────────────────────────────────────
    async def _run(self, *args: str):
        cmd = [
            sys.executable, "-m", "boundflow.cli",
            "--json", "--server", self._server, "--api-key", self._api_key,
            *args,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
        if proc.returncode != 0:
            raise CliError(f"`boundflow {' '.join(args)}` exited {proc.returncode}: {err.decode().strip()}")
        text = out.decode().strip()
        return json.loads(text) if text else None

    # ── tenants ───────────────────────────────────────────────────────────────
    async def create_tenant(self, name: str):
        return SimpleNamespace(**await self._run("tenant", "create", name))

    async def list_tenants(self):
        return [SimpleNamespace(**t) for t in await self._run("tenant", "list")]

    # ── workflows ─────────────────────────────────────────────────────────────
    async def create_workflow(self, workflow_type: str, tenant_id: str, config=None):
        args = ["workflow", "create", workflow_type, tenant_id]
        if config is not None:
            args += ["--version", str(getattr(config, "version", 1))]
            timeout = getattr(config, "invoke_timeout_seconds", None)
            if timeout:
                args += ["--timeout", str(timeout)]
            repeat = getattr(config, "repeat_every_seconds", 0)
            if repeat:
                args += ["--repeat", str(repeat)]
            if not getattr(config, "triggerable", True):
                args += ["--no-triggerable"]
        return SimpleNamespace(**await self._run(*args))

    async def activate_workflow(self, workflow_id: str):
        await self._run("workflow", "activate", workflow_id)

    async def invoke_workflow(self, workflow_id: str, operation_timeout_seconds: int = 0):
        d = await self._run("workflow", "invoke", workflow_id, "--op-timeout", str(operation_timeout_seconds))
        return d["request_id"]

    async def delete_workflow(self, workflow_id: str):
        await self._run("workflow", "delete", workflow_id, "--yes")

    async def get_workflow_lifecycle_state(self, workflow_id: str) -> LifecycleState:
        d = await self._run("workflow", "get", workflow_id)
        return LifecycleState(d["lifecycle_state"])

    async def get_workflow_state(self, workflow_id: str):
        d = await self._run("workflow", "get", workflow_id)
        return WorkflowState(d["workflow_state"]) if d.get("workflow_state") else None

    async def list_workflow_runs(self, workflow_id: str):
        return [SimpleNamespace(**r) for r in await self._run("workflow", "runs", workflow_id)]

    async def get_request_info(self, request_id: str):
        return SimpleNamespace(**await self._run("workflow", "request", request_id))

    async def resolve_interrupted_workflow(self, workflow_id: str, request_id: str):
        await self._run("workflow", "resolve", workflow_id, request_id)

    async def approve_workflow(self, workflow_id: str, approval_id: str, actor: str = ""):
        extra = ["--actor", actor] if actor else []
        await self._run("workflow", "approve", workflow_id, approval_id, *extra)

    async def reject_workflow(self, workflow_id: str, approval_id: str, actor: str = ""):
        extra = ["--actor", actor] if actor else []
        await self._run("workflow", "reject", workflow_id, approval_id, *extra)

    # ── everything else falls back to the real SDK client ─────────────────────
    def __getattr__(self, name):
        # __getattr__ only fires for attributes not defined above.
        return getattr(self._sdk, name)
