"""ctx.agent_model() against a real backend — the metrics a governed loop observes
have to actually reach the server, since lifecycle policy runs off them.

test_governed_model.py covers enforcement against fakes; this covers the wiring:
policy resolved from the server, spend flushed back when the operation ends.
"""
from __future__ import annotations

import pytest

from boundflow import BoundFlowWorker, Complete, RuntimePolicy, WorkflowConfig

from .conftest import (
    WORKER_ADDRESS,
    create_isolated_tenant,
    dummy_mock,
    run_worker,
    wait_for_completion,
)

pytest.importorskip("langchain_core")

from langchain_core.language_models.chat_models import BaseChatModel  # noqa: E402
from langchain_core.messages import AIMessage, HumanMessage  # noqa: E402
from langchain_core.outputs import ChatGeneration, ChatResult  # noqa: E402

AGENT_NAME = "responder"
MODEL = "claude-haiku-4-5-20251001"  # priced by the server; never actually called


class FakeChat(BaseChatModel):
    """Stands in for a real provider so the test needs no API key. Reports usage,
    which is what BoundFlow prices the call from."""
    model_name: str = MODEL

    @property
    def _llm_type(self) -> str:
        return "fake"

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        raise NotImplementedError

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        msg = AIMessage(
            content="done",
            usage_metadata={"input_tokens": 1000, "output_tokens": 200, "total_tokens": 1200},
        )
        return ChatResult(generations=[ChatGeneration(message=msg)])


async def test_governed_loop_metrics_reach_the_server(cp):
    """A customer-driven loop makes 3 calls; the server must end up with those 3
    calls and their cost attributed to the workflow."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("governed_wf", version=1)
    async def _entry(ctx):
        model = ctx.agent_model(AGENT_NAME, FakeChat())
        # Stand-in for "someone else's agent loop" — three calls, our own control flow.
        for _ in range(3):
            await model.ainvoke([HumanMessage(content="hi")])
        return Complete(result={"ok": True})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "governed")
        wf = await cp.create_workflow("governed_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid, timeout=60)
            assert info.status == "completed"

            metrics = await cp.get_workflow_metrics(wf.id)
            assert metrics.total_llm_calls == 3, \
                f"server should have recorded 3 governed calls, got {metrics.total_llm_calls}"
            assert metrics.total_cost_usd > 0, \
                "governed calls must be priced and reported, not recorded as free"
        finally:
            await cp.delete_workflow(wf.id)


async def test_server_side_policy_caps_a_governed_loop(cp):
    """The cap comes from the runtime policy set on the server, not from anything in
    the handler — and tripping it fails the run rather than letting it continue."""
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    calls_made = []

    @worker.workflow("governed_capped_wf", version=1)
    async def _entry(ctx):
        model = ctx.agent_model(AGENT_NAME, FakeChat())
        # Would run away to 10 calls; the server-side cap of 2 has to stop it.
        for _ in range(10):
            await model.ainvoke([HumanMessage(content="hi")])
            calls_made.append(1)
        return Complete(result={"ok": True})

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "governed-capped")
        wf = await cp.create_workflow("governed_capped_wf", tenant.id,
                                      config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_runtime_policy(wf.id, AGENT_NAME, RuntimePolicy(max_llm_calls=2))
            await cp.activate_workflow(wf.id)
            rid = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            info = await wait_for_completion(cp, rid, timeout=60)

            assert len(calls_made) == 2, \
                f"expected the server-side cap to stop the loop at 2, got {len(calls_made)}"
            # Blowing a cap is a customer-domain failure: the run completes as failed
            # rather than crashing the worker, and the workflow stays active.
            assert info.status in ("completed", "failed")

            metrics = await cp.get_workflow_metrics(wf.id)
            assert metrics.total_llm_calls == 2, \
                "spend burned before the cap tripped must still be reported"
        finally:
            await cp.delete_workflow(wf.id)
