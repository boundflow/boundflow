"""Port of ToolCallLimitTests.cs"""
from __future__ import annotations

import boundflow
from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    RuntimePolicy,
    ToolCallLimit,
    WorkflowConfig,
)
from boundflow.anthropic_client import AnthropicLlmClient

from .conftest import HAIKU, WORKER_ADDRESS, create_isolated_tenant, run_worker, wait_for_completion

AGENT_NAME = "limited"


async def test_per_tool_limit_caps_handler_invocations(cp, api_key):
    """
    LLM is instructed to call ping 4 times; runtime policy caps it at 1.
    The customer handler must be invoked at most once.
    """
    ping_calls = [0]

    async def ping_handler(_):
        ping_calls[0] += 1
        return "pong"

    def agent():
        return AgentDefinition(
            name=AGENT_NAME,
            system_prompt=(
                "You are a test agent. You MUST call the `ping` tool 4 separate times, "
                "one at a time, before you finish. After attempting all 4 calls, call submit_result."
            ),
            model=HAIKU,
            tools=[boundflow.Tool("ping", "A no-op ping tool. Call it to register a ping.", ping_handler)],
            output_schema={"done": {"type": "boolean"}},
        )

    worker = BoundFlowWorker(WORKER_ADDRESS, AnthropicLlmClient(api_key))

    @worker.workflow("tool_limit_test", version=1)
    async def _entry(ctx):
        await ctx.run_agent(agent())
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "tool-limit")
        workflow = await cp.create_workflow("tool_limit_test", tenant.id,
                                            config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_runtime_policy(
                workflow.id,
                AGENT_NAME,
                RuntimePolicy(max_llm_calls=8, tool_call_limits=[ToolCallLimit(tool="ping", max_calls=1)]),
            )

            await cp.activate_workflow(workflow.id)
            request_id = await cp.invoke_workflow(workflow.id, operation_timeout_seconds=60)
            await wait_for_completion(cp, request_id)

            assert ping_calls[0] >= 1, "ping handler should have run at least once"
            assert ping_calls[0] <= 1, f"ping handler should have been capped at 1, but ran {ping_calls[0]} times"
        finally:
            await cp.delete_workflow(workflow.id)
