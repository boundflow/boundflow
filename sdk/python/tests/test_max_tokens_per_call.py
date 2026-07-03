"""End-to-end coverage for the max_tokens_per_call runtime policy.

The mock LLM ignores max_tokens, so instead of asserting the model truncates,
we use a spy LlmClient that records the max_tokens it receives. This proves the
full wiring: the policy is snapshotted server-side, delivered to the worker, and
applied to the request by the orchestrator. No Anthropic key needed.
"""
from __future__ import annotations

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    RuntimePolicy,
    WorkflowConfig,
)
from boundflow.llm import SUBMIT_RESULT, LlmResponse, ToolUseBlock, Usage

from .conftest import (
    HAIKU,
    WORKER_ADDRESS,
    create_isolated_tenant,
    run_worker,
    wait_for_completion,
)


class _SpyClient:
    """Records max_tokens per call, then submits to end the agent loop."""

    def __init__(self) -> None:
        self.max_tokens_seen: list[int] = []

    async def complete(self, request):
        self.max_tokens_seen.append(request.max_tokens)
        return LlmResponse(
            content=[ToolUseBlock("toolu_spy", SUBMIT_RESULT, {"summary": "done"})],
            stop_reason="tool_use",
            usage=Usage(10, 5),
        )


def _agent():
    return AgentDefinition(
        name="capped",
        system_prompt="spy agent",
        model=HAIKU,
        output_schema={"summary": {"type": "string"}},
    )


async def _run(cp, spy, policy):
    worker = BoundFlowWorker(WORKER_ADDRESS, spy)

    @worker.workflow("max_tokens_wf", version=1)
    async def _entry(ctx):
        await ctx.run_agent(_agent())
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "max-tokens")
        wf = await cp.create_workflow("max_tokens_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            if policy is not None:
                await cp.set_agent_runtime_policy(wf.id, "capped", policy)
            await cp.activate_workflow(wf.id)
            request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, request_id)
        finally:
            await cp.delete_workflow(wf.id)


async def test_max_tokens_per_call_is_applied_to_the_request(cp):
    spy = _SpyClient()
    await _run(cp, spy, RuntimePolicy(max_tokens_per_call=1234))
    assert spy.max_tokens_seen, "agent never called the LLM"
    assert spy.max_tokens_seen[0] == 1234, f"expected max_tokens=1234, got {spy.max_tokens_seen}"


async def test_max_tokens_defaults_when_policy_unset(cp):
    spy = _SpyClient()
    await _run(cp, spy, RuntimePolicy())  # max_tokens_per_call defaults to 0 (unset)
    assert spy.max_tokens_seen, "agent never called the LLM"
    assert spy.max_tokens_seen[0] == 4096, f"expected default 4096, got {spy.max_tokens_seen}"
