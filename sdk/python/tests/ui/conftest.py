"""Fixtures for the operator console's integration tests.

The sibling suite in tests/test_ui_console.py drives the console against a fake
control plane — fast, and good for the console's own routing. These run it against
the live stack the CLI tests use, so the console is proven against the same server
the CLI is, and a fake that has drifted from the real client can't hide a break.

Each test makes its own tenant and workflows, so they're order-independent.
"""
from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager

import pytest

from boundflow import ControlPlaneClient, MockLlmClient, submit
from boundflow.ui.server import Console, build_app

SERVER_ADDRESS = "http://localhost:50051"
WORKER_ADDRESS = "http://localhost:50052"


@pytest.fixture
def boundflow_api_key():
    key = os.environ.get("BOUNDFLOW_API_KEY")
    if not key:
        pytest.skip("BOUNDFLOW_API_KEY not set")
    return key


@pytest.fixture
def console(boundflow_api_key):
    """A console wired to the live control plane."""
    return Console(SERVER_ADDRESS, boundflow_api_key)


@asynccontextmanager
async def console_client(console: Console):
    """An httpx client over the console, sharing this test's event loop.

    Starlette's TestClient runs its own loop in a thread, which would put the
    console's gRPC channel on a different loop from the test's own control-plane
    client — so this drives the ASGI app directly instead.
    """
    import httpx

    app = build_app(console)
    await console.start()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://console") as client:
            yield client
    finally:
        await console.stop()


@asynccontextmanager
async def control_plane(api_key: str):
    async with ControlPlaneClient(SERVER_ADDRESS, api_key=api_key) as cp:
        yield cp


@asynccontextmanager
async def run_worker(worker):
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def dummy_mock():
    """The worker requires an LlmClient; these workflows never run an agent."""
    return MockLlmClient(lambda _: submit())


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def wait_for(pred, what: str, timeout: float = 30.0):
    """Poll an async predicate until true."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        if await pred():
            return
        assert asyncio.get_event_loop().time() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.25)


async def wait_lifecycle(cp, workflow_id: str, expected, timeout: float = 30.0):
    async def check():
        return (await cp.get_workflow(workflow_id)).lifecycle_state == expected

    await wait_for(check, f"{workflow_id} to reach {expected}", timeout)
