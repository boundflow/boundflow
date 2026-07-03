"""CLI tests for the commands whose effect only exists mid-run: approve, reject,
and resolve's success path. Unlike the rest of the CLI suite these spin up a worker
(the gate/interruption is only reachable with a live run).

The synchronous CliRunner calls asyncio.run() internally, which can't run inside an
async test's loop — so the command under test is invoked via asyncio.to_thread,
which runs it in a threadpool with no active loop while the worker runs in ours.
"""
from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager

from boundflow import (
    AwaitApproval,
    BoundFlowWorker,
    Complete,
    ControlPlaneClient,
    LifecycleState,
    MockLlmClient,
    Next,
    WorkflowConfig,
    submit,
)

from .conftest import SERVER_ADDRESS, run

WORKER_ADDRESS = "http://localhost:50052"


def _dummy_mock():
    return MockLlmClient(lambda _: submit())


@asynccontextmanager
async def _run_worker(worker):
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def _wait(pred, what: str, timeout: int = 30):
    deadline = asyncio.get_event_loop().time() + timeout
    while not pred():
        assert asyncio.get_event_loop().time() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.2)


async def _wait_lifecycle(cp, wf_id: str, expected: LifecycleState, timeout: int = 30):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        state = await cp.get_workflow_lifecycle_state(wf_id)
        if state == expected:
            return
        assert asyncio.get_event_loop().time() < deadline, \
            f"timed out waiting for {expected} on {wf_id}, last={state}"
        await asyncio.sleep(0.3)


async def test_approve_via_cli_runs_approve_branch(runner, boundflow_api_key):
    approve_ran = [False]
    captured = []
    worker = BoundFlowWorker(WORKER_ADDRESS, _dummy_mock())

    @worker.workflow("cli_approve_gate", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="approved_step", context=ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=60,
        )

    @worker.operation("cli_approve_gate", "approved_step")
    async def _approved(ctx):
        approve_ran[0] = True
        return Complete()

    @worker.on_approval_requested
    async def _cap(req):
        captured.append(req)

    async with _run_worker(worker):
        async with ControlPlaneClient(SERVER_ADDRESS, api_key=boundflow_api_key) as cp:
            tenant = await cp.create_tenant(f"cli-approve-{uuid.uuid4().hex[:8]}")
            wf = await cp.create_workflow("cli_approve_gate", tenant.id,
                                          config=WorkflowConfig(version=1))
            try:
                await cp.activate_workflow(wf.id)
                await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
                await _wait(lambda: captured, "the approval gate to open")

                # Approve through the CLI.
                result = await asyncio.to_thread(
                    run, runner, boundflow_api_key,
                    ["workflow", "approve", wf.id, captured[0].approval_id])
                assert result["status"] == "ok"

                await _wait(lambda: approve_ran[0], "the approve branch to run")
            finally:
                await cp.delete_workflow(wf.id)


async def test_reject_via_cli_runs_reject_branch(runner, boundflow_api_key):
    reject_ran = [False]
    approve_ran = [False]
    captured = []
    worker = BoundFlowWorker(WORKER_ADDRESS, _dummy_mock())

    @worker.workflow("cli_reject_gate", version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="approved_step", context=ctx.context, timeout=30),
            on_reject=Next(operation="rejected_step", context=ctx.context, timeout=30),
            timeout=60,
        )

    @worker.operation("cli_reject_gate", "approved_step")
    async def _approved(ctx):
        approve_ran[0] = True
        return Complete()

    @worker.operation("cli_reject_gate", "rejected_step")
    async def _rejected(ctx):
        reject_ran[0] = True
        return Complete()

    @worker.on_approval_requested
    async def _cap(req):
        captured.append(req)

    async with _run_worker(worker):
        async with ControlPlaneClient(SERVER_ADDRESS, api_key=boundflow_api_key) as cp:
            tenant = await cp.create_tenant(f"cli-reject-{uuid.uuid4().hex[:8]}")
            wf = await cp.create_workflow("cli_reject_gate", tenant.id,
                                          config=WorkflowConfig(version=1))
            try:
                await cp.activate_workflow(wf.id)
                await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
                await _wait(lambda: captured, "the approval gate to open")

                result = await asyncio.to_thread(
                    run, runner, boundflow_api_key,
                    ["workflow", "reject", wf.id, captured[0].approval_id])
                assert result["status"] == "ok"

                await _wait(lambda: reject_ran[0], "the reject branch to run")
                assert not approve_ran[0], "approve branch must not run on reject"
            finally:
                await cp.delete_workflow(wf.id)


async def test_resolve_via_cli_reactivates_interrupted_workflow(runner, boundflow_api_key):
    started = asyncio.Event()
    worker = BoundFlowWorker(WORKER_ADDRESS, _dummy_mock())

    @worker.workflow("cli_resolve_gate", version=1)
    async def _entry(ctx):
        started.set()
        await asyncio.sleep(120)  # block so we can drop the worker mid-operation
        return Complete()

    async with ControlPlaneClient(SERVER_ADDRESS, api_key=boundflow_api_key) as cp:
        tenant = await cp.create_tenant(f"cli-resolve-{uuid.uuid4().hex[:8]}")
        wf = await cp.create_workflow("cli_resolve_gate", tenant.id,
                                      config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)

            # Run the worker manually so we can kill it mid-operation (a real drop).
            worker_task = asyncio.create_task(worker.run())
            await asyncio.sleep(0.1)
            request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=120)
            await asyncio.wait_for(started.wait(), timeout=30)
            await asyncio.sleep(2)  # let the server settle into its busy state
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)

            # The lost worker interrupts the workflow.
            await _wait_lifecycle(cp, wf.id, LifecycleState.INTERRUPTED)

            # Resolve through the CLI with the interrupted request id.
            result = await asyncio.to_thread(
                run, runner, boundflow_api_key,
                ["workflow", "resolve", wf.id, request_id])
            assert result["status"] == "ok"

            assert await cp.get_workflow_lifecycle_state(wf.id) == LifecycleState.ACTIVE
        finally:
            await cp.delete_workflow(wf.id)
