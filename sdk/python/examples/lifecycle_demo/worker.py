"""lifecycle_demo/worker.py — the workflow shown in the recording.

Two versions of the same agentic workflow ("order-remediation"):

  - v1 (stable): the analyst agent calls check_refund_eligibility, gets a
    sane amount back, the operation approves it.
  - v2 ("the new deploy"): the analyst calls the NEW tool
    check_refund_eligibility_v2 instead — it's buggy and returns a negative
    max_amount. The operation validates the agent's output and, seeing an
    invalid amount, calls ctx.mark_failed(). That's a customer-reported
    failure, which is what workflow-lifecycle NUM_FAILURES rules watch
    (uncaught exceptions do NOT count — only explicit mark_failed() does).

Run this in its own terminal — its stdout is "the workflow itself's output"
for the recording.

Prereqs
-------
1. Backend up (repo root):  docker compose up -d
2. Provision a key:         docker compose run --rm server -mode=provision -name=lifecycle-demo
                            export BOUNDFLOW_API_KEY=<the printed api_key>
3. Run:                     python worker.py
"""
from __future__ import annotations

import asyncio
import os

from boundflow import (
    AgentDefinition, BoundFlowWorker, Complete, MockContext, MockLlmClient, Tool, Turn, turn,
)
from boundflow.llm import SUBMIT_RESULT, ToolCall

WORKER_ADDRESS = os.environ.get("BOUNDFLOW_WORKER_ADDRESS", "http://localhost:50052")
ORDER_REMEDIATION = "order-remediation"


def submit_amount(amount: float) -> Turn:
    return Turn([ToolCall(SUBMIT_RESULT, {"refund_amount": amount})])


async def check_refund_eligibility(_args: dict) -> dict:
    """v1's tool: known-good, always returns a sane cap."""
    return {"eligible": True, "max_amount": 25.0}


async def check_refund_eligibility_v2(_args: dict) -> dict:
    """v2's NEW tool: the buggy replacement — returns an invalid amount."""
    return {"eligible": True, "max_amount": -999.0}


def mock_script(ctx: MockContext) -> Turn:
    if ctx.turn_index == 0:
        if "[v2]" in ctx.system_prompt:
            return turn(200, 80, "check_refund_eligibility_v2")
        return turn(200, 80, "check_refund_eligibility")
    if "[v2]" in ctx.system_prompt:
        return submit_amount(-999.0)  # naively trusts the broken tool's output
    return submit_amount(25.0)


async def main() -> None:
    worker = BoundFlowWorker(WORKER_ADDRESS, MockLlmClient(mock_script))

    analyst_v1 = AgentDefinition(
        name="analyst",
        system_prompt="[v1] Call check_refund_eligibility, then submit the refund amount.",
        model="mock-model",
        tools=[Tool("check_refund_eligibility", "Look up the refund cap.", check_refund_eligibility)],
        output_schema={"refund_amount": {"type": "number"}},
    )
    analyst_v2 = AgentDefinition(
        name="analyst",
        system_prompt="[v2] Call check_refund_eligibility_v2, then submit the refund amount.",
        model="mock-model",
        tools=[Tool("check_refund_eligibility_v2", "Look up the refund cap (v2).", check_refund_eligibility_v2)],
        output_schema={"refund_amount": {"type": "number"}},
    )

    @worker.workflow(ORDER_REMEDIATION, version=1)
    async def v1(ctx):
        result = await ctx.run_agent(analyst_v1)
        amount = (result.output or {}).get("refund_amount")
        print(f"  [{ORDER_REMEDIATION}] v1 run — refund_amount={amount} — approved", flush=True)
        return Complete()

    @worker.workflow(ORDER_REMEDIATION, version=2)
    async def v2(ctx):
        result = await ctx.run_agent(analyst_v2)
        amount = (result.output or {}).get("refund_amount")
        if amount is None or amount < 0:
            print(f"  [{ORDER_REMEDIATION}] v2 run — refund_amount={amount} — INVALID, marking failed", flush=True)
            ctx.mark_failed()
        else:
            print(f"  [{ORDER_REMEDIATION}] v2 run — refund_amount={amount} — approved", flush=True)
        return Complete()

    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
