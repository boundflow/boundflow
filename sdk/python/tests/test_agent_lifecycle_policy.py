"""Port of AgentLifecyclePolicyTests.cs"""
from __future__ import annotations

from boundflow import (
    AgentDefinition,
    AgentMetric,
    AgentRule,
    BoundFlowWorker,
    Complete,
    Op,
    SetModel,
    WorkflowConfig,
)
from boundflow.anthropic_client import AnthropicLlmClient

from .conftest import (
    HAIKU,
    SONNET,
    WORKER_ADDRESS,
    create_isolated_tenant,
    run_worker,
    wait_for_completion,
)

AGENT_NAME = "analyse"


async def test_model_switches_to_haiku_after_first_invocation(cp, api_key):
    """
    Invoke 1: no prior metrics → rule does not fire → Sonnet is used.
    Invoke 2: invoke-1 metrics satisfy the rule (llm_calls >= 1) → Haiku is used.
    """
    model_results = []

    def analyse_agent():
        return AgentDefinition(
            name=AGENT_NAME,
            system_prompt="You are a concise data analyst.",
            model=SONNET,
            output_schema={"summary": {"type": "string"}},
        )

    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("database_agent", version=1)
    async def _entry(ctx):
        ctx.add_context("Platform context",
                        "BoundFlow is a tenant-aware control plane for safely scheduling "
                        "and running agentic workflows.")
        result = await ctx.run_agent(analyse_agent())
        model_results.append(result.model_used)
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "agent-policy")
        workflow = await cp.create_workflow("database_agent", tenant.id,
                                            config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_lifecycle_policy(
                workflow.id,
                AGENT_NAME,
                [AgentRule(
                    metric=AgentMetric.LLM_CALLS,
                    op=Op.GTE,
                    threshold=1,
                    window=1,
                    action=SetModel(value=HAIKU),
                )],
            )

            await cp.activate_workflow(workflow.id)

            # Invoke 1 — no prior metrics, rule does not fire → Sonnet
            request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, request_id)
            assert len(model_results) == 1, "Invoke 1 did not produce a result"
            assert model_results[0] == SONNET

            # Invoke 2 — invoke-1 metrics trigger the rule → Haiku
            request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, request_id)
            assert len(model_results) == 2, "Invoke 2 did not produce a result"
            assert model_results[1] == HAIKU
        finally:
            await cp.delete_workflow(workflow.id)
