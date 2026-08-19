"""Run deepagents inside BoundFlow, with a durable gate the agent itself triggers.

The seam under test, in one run:

  * the harness's model calls are governed — metered and capped by BoundFlow, which
    never drives the loop
  * the harness's state is durable — filesystem and conversation both survive the
    operation ending, so the task resumes on any worker
  * **the agent asks for approval** on a dangerous tool, the task parks, a human
    decides, and the agent carries on

That third point is the one worth watching. deepagents decides *what* needs a human —
`interrupt_on` here, `permissions(mode="interrupt")` for files. What it can't do is
*wait*: its interrupt is in-process, so the pause dies with the process. BoundFlow parks
the operation instead, and the resume is a separate operation that may land on a
different machine.

The division that emerged, worth stating because it decides every ambiguous case:

  deepagents  — which tools exist, per-action allow/deny/interrupt, per-run call limits
  BoundFlow   — where policy comes from, durable waiting, and metrics across runs and
                versions (the pause / cooldown / rollback the harness has no notion of)

Run against a local stack:
    docker compose up -d --wait          # plus the override publishing 5433
    export ANTHROPIC_API_KEY=...  BOUNDFLOW_API_KEY=...
    python -m examples.deepagents_harness
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

from deepagents import create_deep_agent
from deepagents.backends import StoreBackend
from langchain_anthropic import ChatAnthropic
from langchain_core.tools import tool
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.postgres.aio import AsyncPostgresStore
from langgraph.types import Command

from boundflow import (
    AwaitApproval,
    CapabilityCallLimit,
    FileRule,
    RuntimePolicy,
    BoundFlowWorker,
    Complete,
    ControlPlaneClient,
    MockLlmClient,
    Next,
    WorkflowConfig,
    submit,
)
from boundflow.capabilities import file_permissions
from boundflow.harness_callbacks import governed_tool_callbacks
from boundflow.harness_gates import approve, pending_action, reject
from boundflow.harness_middleware import harness_middleware

STORE_URL = os.environ.get(
    "BOUNDFLOW_STORE_URL", "postgres://boundflow:boundflow@localhost:5433/boundflow")
MODEL = "claude-sonnet-4-6"
WORKFLOW = "deepagents_harness"
AGENT = "operator"

SYSTEM = ("You are an infrastructure agent. Keep working notes in notes.md. "
          "Use restart_database when asked to restart a database.")

# The rules an engineer would otherwise hard-code where the agent is built. As policy
# they arrive with the operation, version with the agent, and roll back with it.
POLICY = RuntimePolicy(
    max_llm_calls=8,
    # Not `write_file`: cap the capability, or the agent switches to `edit_file`.
    capability_call_limits=[CapabilityCallLimit(capability="write", max_calls=3)],
    file_rules=[FileRule(operations=["write"], paths=["/secrets/**"], mode="deny")],
)


@tool
def restart_database(name: str) -> str:
    """Restart a production database. Disruptive."""
    return f"database {name} restarted"


def build_worker() -> BoundFlowWorker:
    # The harness brings its own model, so BoundFlow's orchestrator never runs — but the
    # constructor still demands an LlmClient. Worth removing.
    worker = BoundFlowWorker(llm=MockLlmClient(lambda _: submit()))

    async def _run(ctx, task_id: str, payload):
        """One governed round. `payload` is either a fresh message or a `Command`
        resuming a parked interrupt — the harness treats both as an invocation.

        Two independent things make this resumable: `thread_id` continues the same
        conversation, and the store namespace is the same filesystem.
        """
        governor = ctx.agent_governor(AGENT)
        governor.register_harness_observer()

        async with (
            AsyncPostgresStore.from_conn_string(STORE_URL) as store,
            AsyncPostgresSaver.from_conn_string(STORE_URL) as saver,
        ):
            await store.setup()
            await saver.setup()
            backend = StoreBackend(
                namespace=lambda _rt: ("default", WORKFLOW, task_id), store=store)

            result = await ctx.run_governed(
                AGENT,
                lambda m, t: create_deep_agent(
                    model=m, tools=t, backend=backend, checkpointer=saver,
                    # Both of these are the agent's RuntimePolicy, translated. Ours to
                    # declare and version; theirs to enforce.
                    permissions=file_permissions(governor.policy),
                    middleware=harness_middleware(governor),
                    # Likewise the decision to pause: works on any tool, and BoundFlow
                    # adds nothing to it. What it adds is the waiting.
                    interrupt_on={"restart_database": True},
                    system_prompt=SYSTEM,
                ).ainvoke(payload, {
                    "configurable": {"thread_id": task_id},
                    # Metering rides the callbacks, so it reaches subagents too — a
                    # `task` call's tools are counted, which middleware wouldn't see.
                    "callbacks": [governed_tool_callbacks(governor)],
                }),
                chat_model=ChatAnthropic(model=MODEL, max_tokens=1024),
                tools=[restart_database],
            )
        print(f"  [round] llm_calls={result.llm_calls_used} "
              f"cost=${result.cost_usd:.4f} tools={result.calls_per_tool}")
        return result

    @worker.workflow(WORKFLOW, version=1)
    async def entry(ctx):
        task_id = ctx._op.request_id
        result = await _run(ctx, task_id, {"messages": [{
            "role": "user",
            # The restart comes first so the gate is reached on the opening round —
            # asked the other way round the model sometimes writes its notes, reports
            # back, and never gets to the tool that needs a human.
            "content": ("db-prod-1 is unhealthy. Restart it, then note what you did in "
                        "notes.md and copy that note to /secrets/backup.md.")}]})

        action = pending_action(result)
        if action is None:
            return Complete(result={"finished_without_asking": True})

        # The agent asked. Park here: the worker may now die, and the decision plus the
        # resume are a separate operation.
        print(f"  [gate] agent requested: {action['name']}({action['args']})")
        return AwaitApproval(
            on_approve=Next("resume", context={"task_id": task_id, "decision": approve()},
                            timeout=300),
            on_reject=Next("resume",
                           context={"task_id": task_id,
                                    "decision": reject("not during business hours")},
                           timeout=300),
            justification=action["description"],
            metadata={"tool": action["name"], "args": action["args"]},
            timeout=86_400,
        )

    @worker.operation(WORKFLOW, "resume")
    async def resume(ctx):
        """A separate operation, possibly a different worker. The interrupt lives in the
        checkpointer, so handing the decision back is all it takes."""
        result = await _run(ctx, ctx.context["task_id"],
                            Command(resume=ctx.context["decision"]))
        messages = (result.output or {}).get("messages", [])
        final = str(getattr(messages[-1], "content", "")) if messages else ""
        return Complete(result={"final": final[:300]})

    return worker


async def serve() -> None:
    """Just a worker. Nothing about it is specific to a task or a machine — it claims
    whatever the control plane hands it."""
    print(f"worker {os.getpid()} up")
    await build_worker().run()


async def start() -> tuple[str, str]:
    """Create the workflow, arm the policy, invoke, and wait for the agent to park."""
    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant(f"harness-{uuid.uuid4().hex[:8]}")
        wf = await cp.create_workflow(WORKFLOW, tenant.id, config=WorkflowConfig(version=1))
        await cp.set_agent_runtime_policy(wf.id, AGENT, POLICY)
        await cp.activate_workflow(wf.id)
        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=300)
        print(f"invoked {request_id} on workflow {wf.id}")

        while not (info := await cp.get_workflow(wf.id)).pending_approval:
            await asyncio.sleep(1)
        print(f"  parked on: {info.pending_approval.metadata}")
        return wf.id, request_id


async def finish(workflow_id: str, request_id: str) -> None:
    """Approve, and wait for whichever worker is alive to carry the task home."""
    async with ControlPlaneClient() as cp:
        info = await cp.get_workflow(workflow_id)
        pending = info.pending_approval
        await cp.approve_workflow(workflow_id, pending.approval_id, actor="operator@corp")
        print("  approved")
        while not (final := await cp.get_request_info(request_id)).status.is_terminal():
            await asyncio.sleep(1)
        print(f"  done: {final.status.value}")


async def main() -> None:
    """One process doing everything — the simple demo."""
    task = asyncio.create_task(build_worker().run())
    await asyncio.sleep(0.5)
    workflow_id, request_id = await start()
    await finish(workflow_id, request_id)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    # `serve` / `start` / `finish` exist so the three roles can be separate processes:
    #   python -m examples.deepagents_harness serve   &      # worker A
    #   python -m examples.deepagents_harness start          # parks, prints workflow id
    #   kill %1; python -m examples.deepagents_harness serve &   # worker B, knows nothing
    #   python -m examples.deepagents_harness finish <workflow-id> <request-id>
    # Worker B finishes a task it never started, which is the claim worth testing.
    mode = sys.argv[1] if len(sys.argv) > 1 else "demo"
    if mode == "serve":
        asyncio.run(serve())
    elif mode == "start":
        asyncio.run(start())
    elif mode == "finish":
        asyncio.run(finish(sys.argv[2], sys.argv[3]))
    else:
        asyncio.run(main())
