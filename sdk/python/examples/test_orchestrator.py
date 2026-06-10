"""Proves the SDK backend works end-to-end, no gRPC/server needed.

Drives OperationContext.run_agent exactly as the worker stream would: we fake
the operation-context JSON the Go server injects (agentRuntimePolicies +
agentStates), run the agent, and assert the lifecycle policy mutates the model.
"""

import asyncio

from boundflow import (
    AgentDefinition, MockContext, MockLlmClient, Tool, turn, submit,
)
from boundflow.worker import OperationContext
from boundflow.llm import Orchestrator

SONNET, OPUS = "claude-sonnet-4-6", "claude-opus-4-8"


class FakeOp:
    def __init__(self, context):
        self.name = "reconcile_entry"
        self.workflow_version = 1
        self.context = context


async def ok(_):
    return "ok"


def analyst() -> AgentDefinition:
    return AgentDefinition(
        name="analyst",
        system_prompt="[order-analyze] diagnose stuck orders",
        model=SONNET,
        tools=[Tool("get_order_status", "", ok), Tool("retry_fulfillment_job", "", ok)],
        output_schema={"summary": {"type": "string"}},
    )


# Mock: first call loops on retry_fulfillment_job x4, then submits.
def script(looping):
    def fn(ctx: MockContext):
        if ctx.turn_index == 0:
            if looping[0]:
                return turn(400, 200, *(["retry_fulfillment_job"] * 4))
            return turn(400, 200, "get_order_status")
        return submit()
    return fn


async def main():
    looping = [True]
    orch = Orchestrator(MockLlmClient(script(looping)))

    # ── Run 1: empty history, looping. Policy: calls_per_tool[retry] >= 3 → Opus.
    lifecycle_policy = {
        "rules": [{
            "metric": "calls_per_tool", "tool": "retry_fulfillment_job",
            "op": "greater_than_or_equal", "threshold": 3, "window": 1,
            "action": {"field": "model", "value": OPUS},
        }]
    }
    ctx1 = OperationContext(FakeOp({
        "agentRuntimePolicies": {"analyst": {"max_llm_calls": 2}},
        "agentStates": {"analyst": {"lifecycle_policy": lifecycle_policy, "invocation_metrics": []}},
    }), orch)
    r1 = await ctx1.run_agent(analyst())
    snap1 = ctx1.agent_state_updates["analyst"]
    print(f"  run 1: model={r1.model_used}  retry_calls={snap1['calls_per_tool'].get('retry_fulfillment_job')}")
    assert r1.model_used == SONNET, "first run should still be Sonnet"
    assert snap1["calls_per_tool"]["retry_fulfillment_job"] == 4

    # ── Run 2: history now carries run 1's snapshot. Policy should fire → Opus.
    looping[0] = False
    ctx2 = OperationContext(FakeOp({
        "agentRuntimePolicies": {"analyst": {"max_llm_calls": 2}},
        "agentStates": {"analyst": {"lifecycle_policy": lifecycle_policy, "invocation_metrics": [snap1]}},
    }), orch)
    r2 = await ctx2.run_agent(analyst())
    print(f"  run 2: model={r2.model_used}  ← lifecycle policy escalated")
    assert r2.model_used == OPUS, "second run should escalate to Opus"

    print("\n  ✓ loop detection → model escalation works end-to-end")
    await cost_and_failures()


async def cost_and_failures():
    HAIKU = "claude-haiku-4-5-20251001"

    # ── Cost-spike downgrade: prior run cost ≥ $1 → switch to Haiku. ──────────
    def spiky(ctx: MockContext):
        return turn(500_000, 200_000, "get_order_status") if ctx.turn_index == 0 else submit()

    orch = Orchestrator(MockLlmClient(spiky))
    agent = AgentDefinition(name="analyst", system_prompt="[order-analyze] x", model=SONNET,
                            tools=[Tool("get_order_status", "", ok)],
                            output_schema={"summary": {"type": "string"}})
    cost_policy = {"rules": [{
        "metric": "cost_usd", "op": "greater_than_or_equal", "threshold": 1, "window": 1,
        "action": {"field": "model", "value": HAIKU},
    }]}
    ctx_a = OperationContext(FakeOp({
        "agentRuntimePolicies": {"analyst": {}},
        "agentStates": {"analyst": {"lifecycle_policy": cost_policy, "invocation_metrics": []}},
    }), orch)
    ra = await ctx_a.run_agent(agent)
    snap = ctx_a.agent_state_updates["analyst"]
    ctx_b = OperationContext(FakeOp({
        "agentRuntimePolicies": {"analyst": {}},
        "agentStates": {"analyst": {"lifecycle_policy": cost_policy, "invocation_metrics": [snap]}},
    }), orch)
    rb = await ctx_b.run_agent(agent)
    print(f"  cost run: ${snap['cost_usd']:.2f} on {ra.model_used} → next run {rb.model_used}")
    assert rb.model_used == HAIKU

    # ── Tool-failure counting: a throwing handler increments failure counts. ──
    async def boom(_):
        raise RuntimeError("smart_retry_v2: downstream unavailable")

    def v2(ctx: MockContext):
        return turn(400, 200, "smart_retry_v2") if ctx.turn_index == 0 else submit()

    orch2 = Orchestrator(MockLlmClient(v2))
    v2_agent = AgentDefinition(name="analyst", system_prompt="[order-analyze-v2] x", model=SONNET,
                               tools=[Tool("smart_retry_v2", "", boom)],
                               output_schema={"summary": {"type": "string"}})
    ctx_c = OperationContext(FakeOp({"agentRuntimePolicies": {}, "agentStates": {}}), orch2)
    rc = await ctx_c.run_agent(v2_agent)
    print(f"  flaky tool: tool_failure_counts={rc.tool_failure_counts}")
    assert rc.tool_failure_counts.get("smart_retry_v2") == 1

    print("\n  ✓ cost downgrade + tool-failure counting work end-to-end")


if __name__ == "__main__":
    asyncio.run(main())
