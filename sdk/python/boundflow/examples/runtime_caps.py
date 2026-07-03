"""Agent runtime policy — a hard cap enforced *during* a run.

Runtime limits (`max_cost_usd`, `max_llm_calls`, `max_tokens_per_call`, per-tool
caps) are enforced by the SDK while the agent runs, so a runaway or looping agent
is stopped mid-flight. Here a (mock) agent loops forever calling a tool with heavy
token usage; a $0.10 cost cap halts it after a few turns.

Deterministic — no Anthropic key needed. Prereqs: backend up + BOUNDFLOW_API_KEY.
Run:  python -m boundflow.examples.runtime_caps
"""
import asyncio

from boundflow import (
    AgentDefinition, BoundFlowWorker, Complete, ControlPlaneClient, MockContext,
    MockLlmClient, RuntimePolicy, Tool, Turn, WorkflowConfig, turn,
)

AGENT = "looper"


def _loop_forever(ctx: MockContext) -> Turn:
    # Never submit: keep calling `ping` with heavy token usage so the run's cost
    # climbs every turn — the runtime cap has to be what stops the loop.
    return turn(20_000, 5_000, "ping")


async def main() -> None:
    ping_calls = [0]

    async def ping(_):
        ping_calls[0] += 1
        return "pong"

    worker = BoundFlowWorker(llm=MockLlmClient(_loop_forever))

    @worker.workflow("runtime_caps", version=1)
    async def _entry(ctx):
        result = await ctx.run_agent(AgentDefinition(
            name=AGENT,
            system_prompt="Keep calling ping.",
            model="claude-haiku-4-5",
            tools=[Tool("ping", "A no-op ping tool.", ping)],
            output_schema={"done": {"type": "boolean"}},
        ))
        print(f"  agent halted after {result.llm_calls_used} LLM call(s), "
              f"cost ${result.cost_usd:.4f} (ping ran {ping_calls[0]}x)")
        return Complete()

    task = asyncio.create_task(worker.run())
    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant("runtime-caps")
        wf = await cp.create_workflow("runtime_caps", tenant.id, config=WorkflowConfig(version=1))
        try:
            # Hard caps enforced during the run. max_llm_calls is a backstop so the
            # loop can't run away even if the cost math is off.
            await cp.set_agent_runtime_policy(
                wf.id, AGENT, RuntimePolicy(max_cost_usd=0.10, max_llm_calls=25),
            )
            await cp.activate_workflow(wf.id)
            await _wait_done(cp, await cp.invoke_workflow(wf.id, operation_timeout_seconds=60))
            print("  → the runtime cost cap stopped a loop that would never end on its own.")
        finally:
            await cp.delete_workflow(wf.id)
    task.cancel()


async def _wait_done(cp, request_id, timeout=90):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        info = await cp.get_request_info(request_id)
        if info.status.is_terminal():
            return info
        assert asyncio.get_event_loop().time() < deadline, "timed out waiting for the run"
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
