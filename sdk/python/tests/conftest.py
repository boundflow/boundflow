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


@pytest_asyncio.fixture
async def cp():
    async with ControlPlaneClient(SERVER_ADDRESS) as client:
        yield client


def dummy_mock():
    return MockLlmClient(lambda _: submit())


async def create_isolated_tenant(cp: ControlPlaneClient, prefix: str = "test"):
    uid = uuid.uuid4().hex[:8]
    tenant = await cp.create_tenant(f"{prefix}-tenant-{uid}")
    return tenant


async def wait_for_completion(cp: ControlPlaneClient, workflow_id: str, timeout: int = 120) -> LifecycleState:
    """Mirror C# WaitForCompletionAsync: delay THEN poll, so we never return ACTIVE
    before the server has had time to transition through INVOKING."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        await asyncio.sleep(0.5)  # delay first, exactly like C# do { Task.Delay(500); check } while
        state = await cp.get_workflow_lifecycle_state(workflow_id)
        if state != LifecycleState.INVOKING:
            return state
        assert asyncio.get_event_loop().time() < deadline, f"Timed out waiting for completion of {workflow_id}"


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
