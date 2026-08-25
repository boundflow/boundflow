"""RuntimePolicy.custom — caps BoundFlow carries but never enforces.

The point of the field is that the *caller* enforces it, so the test that matters is the
whole loop: declare it, round-trip it through the server, read it back inside a handler,
and act on it. A value BoundFlow stores and nobody can read would be worse than nothing.
"""
from __future__ import annotations

from boundflow import (
    BoundFlowWorker,
    Complete,
    RuntimePolicy,
    WorkflowConfig,
)

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
)


async def test_custom_policy_round_trips_through_get(cp):
    """Stored opaquely and handed back verbatim — nested values and all."""
    tenant = await create_isolated_tenant(cp, "custom-rt")
    wf = await cp.create_workflow("custom_rt", tenant.id, config=WorkflowConfig(version=1))

    declared = {"max_total_subagents": 4, "allowed_spawns": ["researcher", "writer"],
                "approval_required": [{"tool": "desk__create_refund", "timeout_seconds": 900}]}
    await cp.set_agent_runtime_policy(
        wf.id, "analyst", RuntimePolicy(max_cost_usd=0.25, custom=declared))

    got = await cp.get_agent_runtime_policy(wf.id, "analyst")
    assert got["custom"] == declared
    # The enforced caps still work as before — custom sits alongside, not instead.
    assert got["max_cost_usd"] == 0.25


async def test_handler_reads_custom_policy_and_enforces_it(cp):
    """A handler enforcing its own cap, which is the entire contract of the field."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("custom_enforce", version=1)
    async def _entry(ctx):
        policy = ctx.policy("analyst")
        cap = policy.custom.get("max_total_subagents", 0)
        spawned = 0
        # Whatever "spawn" means to the caller — BoundFlow has no idea, which is the point.
        for _ in range(10):
            if cap and spawned >= cap:
                break
            spawned += 1
        return Complete(result={"cap": cap, "spawned": spawned,
                                "enforced_cap_also_visible": policy.max_cost_usd})

    tenant = await create_isolated_tenant(cp, "custom-enf")
    wf = await cp.create_workflow("custom_enforce", tenant.id, config=WorkflowConfig(version=1))
    await cp.set_agent_runtime_policy(
        wf.id, "analyst", RuntimePolicy(max_cost_usd=0.5, custom={"max_total_subagents": 3}))
    await cp.activate_workflow(wf.id)

    async with run_worker(worker):
        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)
        info = await wait_for_completion(cp, request_id, timeout=60)

    assert info.result == {"cap": 3, "spawned": 3, "enforced_cap_also_visible": 0.5}


async def test_custom_defaults_empty_and_is_never_enforced(cp):
    """Unset is an empty dict, not None — a handler can read it without guarding. And an
    unrecognised cap does nothing on its own: nothing in BoundFlow acts on it."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("custom_default", version=1)
    async def _entry(ctx):
        return Complete(result={"custom": ctx.policy("analyst").custom})

    tenant = await create_isolated_tenant(cp, "custom-def")
    wf = await cp.create_workflow("custom_default", tenant.id, config=WorkflowConfig(version=1))
    # A cap that would stop the run dead if anything enforced it. Nothing does.
    await cp.set_agent_runtime_policy(
        wf.id, "analyst", RuntimePolicy(custom={"max_seconds": 0.001}))
    await cp.activate_workflow(wf.id)

    async with run_worker(worker):
        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)
        info = await wait_for_completion(cp, request_id, timeout=60)

    assert info.status == "completed", "an unenforced cap must not affect the run"
    assert info.result == {"custom": {"max_seconds": 0.001}}
