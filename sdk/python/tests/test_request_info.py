"""get_request_info surfaces what was actually resolved for that specific run --
the invoke-time context, the resolved operation timeout, and each agent's resolved
runtime policy -- so a customer can see what was true at the time a run happened,
independent of whatever the workflow/agent config has since changed to."""
from __future__ import annotations

from boundflow import AgentDefinition, BoundFlowWorker, Complete, RuntimePolicy, WorkflowConfig

from .conftest import WORKER_ADDRESS, create_isolated_tenant, dummy_mock, run_worker, wait_for_completion


async def test_get_request_info_surfaces_invoke_context_timeout_and_agent_policies(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("request_info_test", version=1)
    async def _entry(ctx):
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "request-info")
        wf = await cp.create_workflow("request_info_test", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)

        await cp.set_agent_runtime_policy(wf.id, "researcher", RuntimePolicy(max_llm_calls=7))

        request_id = await cp.invoke_workflow(
            wf.id, operation_timeout_seconds=42, context={"customer_order_id": "ord_123"})
        await wait_for_completion(cp, request_id)

        info = await cp.get_request_info(request_id)
        assert info.invoke_context == {"customer_order_id": "ord_123"}
        assert info.timeout_seconds == 42
        assert info.agent_runtime_policies["researcher"]["max_llm_calls"] == 7


async def test_get_request_info_invoke_context_none_when_not_provided(cp):
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow("request_info_no_context", version=1)
    async def _entry(ctx):
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "request-info-none")
        wf = await cp.create_workflow("request_info_no_context", tenant.id, config=WorkflowConfig(version=1))
        await cp.activate_workflow(wf.id)

        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
        await wait_for_completion(cp, request_id)

        info = await cp.get_request_info(request_id)
        assert info.invoke_context is None
