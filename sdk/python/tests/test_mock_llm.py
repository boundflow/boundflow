"""Port of MockLlmTests.cs"""
from __future__ import annotations

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    MockLlmClient,
    MockContext,
    RuntimePolicy,
    ToolCallLimit,
    Turn,
    WorkflowConfig,
    submit,
    turn,
)

from .conftest import WORKER_ADDRESS, create_isolated_tenant, run_worker, wait_for_completion

AGENT_NAME = "mocked"


async def test_mock_llm_drives_tool_calls_and_per_tool_limit_holds(cp):
    """
    Scripted mock calls ping 3 times then submits. With a per-tool cap of 1, the
    handler must fire only once — proving both mock wiring and limit enforcement.
    """
    ping_calls = [0]

    def mock_fn(ctx: MockContext) -> Turn:
        if ctx.turn_index < 3:
            return turn(100, 50, "ping")
        return submit()

    async def ping_handler(_):
        ping_calls[0] += 1
        return "pong"

    def agent():
        return AgentDefinition(
            name=AGENT_NAME,
            system_prompt="mock agent",
            model="mock-model",
            tools=[
                __import__("boundflow").Tool("ping", "ping", ping_handler),
            ],
            output_schema={"done": {"type": "boolean"}},
        )

    worker = BoundFlowWorker(WORKER_ADDRESS, MockLlmClient(mock_fn))

    @worker.workflow("mock_tool_limit", version=1)
    async def _entry(ctx):
        await ctx.run_agent(agent())
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "mock-tool-limit")
        workflow = await cp.create_workflow("mock_tool_limit", tenant.id,
                                            config=WorkflowConfig(version=1))

        await cp.set_agent_runtime_policy(
            workflow.id,
            AGENT_NAME,
            RuntimePolicy(max_llm_calls=8, tool_call_limits=[ToolCallLimit(tool="ping", max_calls=1)]),
        )

        await cp.activate_workflow(workflow.id)
        await cp.invoke_workflow(workflow.id, operation_timeout_seconds=30)
        await wait_for_completion(cp, workflow.id)

        assert ping_calls[0] == 1, f"ping handler should have been called exactly once, got {ping_calls[0]}"
