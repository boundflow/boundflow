"""invoke_workflow(context=...) seeds the context the operations read and write via
ctx.context — the caller's own data, kept apart from the runtime's keys (agentStates,
modelPricing, …) that share the raw job context. It rides request_info -> the job
context (set once at scheduling, like correlationId), so it's there at the entry op and
carries through chained ops that hand ctx.context to the next operation."""
from __future__ import annotations

from boundflow import (
    BoundFlowWorker,
    Complete,
    Next,
    WorkflowConfig,
)

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
)


async def test_invoke_context_readable_via_ctx_context(cp):
    seen: dict = {}
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("inv_ctx", version=1)
    async def _entry(ctx):
        seen["context"] = dict(ctx.context)
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "inv-ctx")
        wf = await cp.create_workflow("inv_ctx", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30,
                                           context={"ticket": "T-42", "note": "urgent"})
            await wait_for_completion(cp, rid)
            assert seen["context"] == {"ticket": "T-42", "note": "urgent"}
        finally:
            await cp.delete_workflow(wf.id)


async def test_context_is_isolated_from_system_keys(cp):
    """ctx.context holds only the caller's data — the runtime's keys
    (agentRuntimePolicies, correlationId, …) stay out of it."""
    seen: dict = {}
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("inv_iso", version=1)
    async def _entry(ctx):
        seen["keys"] = sorted(ctx.context.keys())
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "inv-iso")
        wf = await cp.create_workflow("inv_iso", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30,
                                           context={"ticket": "T-42"})
            await wait_for_completion(cp, rid)
            assert seen["keys"] == ["ticket"]   # only the caller's key, no system keys
        finally:
            await cp.delete_workflow(wf.id)


async def test_context_available_in_chained_op(cp):
    """Handing ctx.context to the next op via Next carries the caller's data forward —
    and a write made along the way rides with it, landing back in ctx.context."""
    seen: dict = {}
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("inv_ctx_chain", version=1)
    async def _entry(ctx):
        seen["entry"] = dict(ctx.context)
        ctx.context["step"] = "one"
        return Next(operation="step2", context=ctx.context, timeout=30)

    @worker.operation("inv_ctx_chain", "step2")
    async def _step2(ctx):
        seen["step2"] = dict(ctx.context)
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "inv-ctx-chain")
        wf = await cp.create_workflow("inv_ctx_chain", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30,
                                           context={"session": "abc"})
            await wait_for_completion(cp, rid)
            assert seen["entry"] == {"session": "abc"}
            assert seen["step2"] == {"session": "abc", "step": "one"}
        finally:
            await cp.delete_workflow(wf.id)


async def test_invoke_without_context_gives_empty_context(cp):
    seen: dict = {}
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("inv_noctx", version=1)
    async def _entry(ctx):
        seen["context"] = dict(ctx.context)
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "inv-noctx")
        wf = await cp.create_workflow("inv_noctx", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, rid)
            assert seen["context"] == {}
        finally:
            await cp.delete_workflow(wf.id)
