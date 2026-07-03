"""Shared fixtures and helpers for BoundFlow integration tests."""
from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio

from boundflow import (
    ControlPlaneClient,
    LifecycleState,
    MockLlmClient,
    WorkflowState,
    submit,
)

WORKER_ADDRESS = "http://localhost:50052"
SERVER_ADDRESS = "http://localhost:50051"
SONNET = "claude-sonnet-4-6"
HAIKU = "claude-haiku-4-5-20251001"


@pytest.fixture
def api_key():
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        pytest.skip("ANTHROPIC_API_KEY not set")
    return key


@pytest.fixture
def boundflow_api_key():
    key = os.environ.get("BOUNDFLOW_API_KEY")
    if not key:
        pytest.skip("BOUNDFLOW_API_KEY not set")
    return key


@pytest_asyncio.fixture
async def cp(boundflow_api_key):
    async with ControlPlaneClient(SERVER_ADDRESS, api_key=boundflow_api_key) as client:
        yield client


def dummy_mock():
    return MockLlmClient(lambda _: submit())


async def create_isolated_tenant(cp: ControlPlaneClient, prefix: str = "test"):
    uid = uuid.uuid4().hex[:8]
    tenant = await cp.create_tenant(f"{prefix}-tenant-{uid}")
    return tenant


async def wait_for_completion(cp: ControlPlaneClient, request_id: str, timeout: int = 120):
    """Poll a specific run (by the request_id invoke returned) until it is terminal, and return
    its final RequestInfo. Keyed to the run — not the workflow's aggregate lifecycle — so it
    can't false-positive on the pre-scheduled window."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        info = await cp.get_request_info(request_id)
        if info.status in ("completed", "failed"):
            return info
        assert asyncio.get_event_loop().time() < deadline, f"Timed out waiting for run {request_id}"
        await asyncio.sleep(0.5)


async def wait_for_lifecycle_state(
    cp: ControlPlaneClient, workflow_id: str, expected: LifecycleState, timeout: int = 120
) -> LifecycleState:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        state = await cp.get_workflow_lifecycle_state(workflow_id)
        if state == expected:
            return state
        assert asyncio.get_event_loop().time() < deadline, \
            f"Timed out waiting for {expected} on {workflow_id}, last={state}"
        await asyncio.sleep(0.5)


async def wait_for_workflow_state(
    cp: ControlPlaneClient, workflow_id: str, expected: WorkflowState, timeout: int = 120
) -> WorkflowState:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        state = await cp.get_workflow_state(workflow_id)
        if state == expected:
            return state
        assert asyncio.get_event_loop().time() < deadline, \
            f"Timed out waiting for {expected} on {workflow_id}, last={state}"
        await asyncio.sleep(0.5)


@asynccontextmanager
async def run_worker(worker):
    """Run a BoundFlowWorker as a background task, cancel it on exit."""
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
