"""BoundFlow demo — Acme's agentic workflows under governance (Python).

Faithful port of sdk/dotnet/demo/BoundFlow.Demo/Program.cs. No ANTHROPIC_API_KEY:
a scripted mock LLM keeps cost / call-counts / tool-calls / failures deterministic.

Interactive like the C# demo (keypress to advance, y/n to approve). Set
BOUNDFLOW_DEMO_AUTO=1 to run unattended (auto-advance, auto-approve, auto-reject
the pause step).
"""

import asyncio
import os
import sys
from dataclasses import dataclass

if sys.platform == "win32":
    import msvcrt
else:
    import termios
    import tty

import boundflow as bf
from boundflow import (
    AgentDefinition, AgentMetric, AgentRule, AwaitApproval, BoundFlowWorker,
    Complete, ControlPlaneClient, Cooldown, LifecycleState, MockContext,
    MockLlmClient, Next, Op, Pause, RuntimePolicy, SetModel, SetVersion,
    ToolCallLimit, Turn, WorkflowConfig, WorkflowMetric, WorkflowRule,
    WorkflowState, submit, tool, turn,
)

WORKER_ADDR = "http://localhost:50052"
SERVER_ADDR = "http://localhost:50051"

SONNET = "claude-sonnet-4-6"
HAIKU = "claude-haiku-4-5-20251001"
OPUS = "claude-opus-4-8"

AUTO = os.environ.get("BOUNDFLOW_DEMO_AUTO") == "1"


@dataclass
class DemoState:
    mode: str = "normal"          # normal | failing | rejecting
    cost_spike: bool = False
    looping: bool = False
    skip_approval: bool = False


state = DemoState()
approvals: dict = {}


# ── Mock LLM: scripted per agent (branch on the system-prompt tag) ───────────
def script(ctx: MockContext) -> Turn:
    sp = ctx.system_prompt

    if "[ticket-triage]" in sp:
        return turn(300, 150, "get_ticket", "search_kb") if ctx.turn_index == 0 else submit()

    if "[order-analyze]" in sp:
        if ctx.turn_index == 0:
            if state.cost_spike:
                return turn(500_000, 200_000, "get_order_status", "get_queue_depth")
            if state.looping:
                return turn(400, 200, *(["retry_fulfillment_job"] * 4))
            return turn(400, 200, "get_order_status", "get_queue_depth")
        return submit()

    if "[order-analyze-v2]" in sp:
        return turn(400, 200, "get_order_status", "smart_retry_v2") if ctx.turn_index == 0 else submit()

    if "[order-rollback]" in sp:
        return turn(300, 150, "rollback_fulfillment_config") if ctx.turn_index == 0 else submit()

    if "[incident-diagnosis]" in sp:
        return turn(400, 200, "get_service_health", "get_recent_deployments", "open_incident_ticket") \
            if ctx.turn_index == 0 else submit()

    return submit()


# ── Tools ────────────────────────────────────────────────────────────────────
def read_tool(name: str) -> bf.Tool:
    async def handler(_input: dict):
        return "ok"
    return bf.Tool(name=name, description=name, handler=handler)


def flaky_tool(name: str) -> bf.Tool:
    async def handler(_input: dict):
        raise RuntimeError(f"{name}: downstream service unavailable")
    return bf.Tool(name=name, description=name, handler=handler)


# ── Agents ───────────────────────────────────────────────────────────────────
SCHEMA = {"summary": {"type": "string"}}

triage_agent = AgentDefinition(
    name="triage",
    system_prompt="[ticket-triage] You are a support triage agent. Read-only tools only.",
    model=SONNET,
    tools=[read_tool("get_ticket"), read_tool("search_kb")],
    output_schema=SCHEMA,
)

analyze_agent = AgentDefinition(
    name="analyst",
    system_prompt="[order-analyze] You diagnose stuck orders. Retry is allowed; pause/rollback need approval.",
    model=SONNET,
    tools=[read_tool("get_order_status"), read_tool("get_queue_depth"),
           read_tool("get_worker_logs"), read_tool("retry_fulfillment_job")],
    output_schema=SCHEMA,
)

analyze_v2_agent = AgentDefinition(
    name="analyst",
    system_prompt="[order-analyze-v2] Diagnose stuck orders using the new smart retry service.",
    model=SONNET,
    tools=[read_tool("get_order_status"), read_tool("get_queue_depth"), flaky_tool("smart_retry_v2")],
    output_schema=SCHEMA,
)

rollback_agent = AgentDefinition(
    name="rollback",
    system_prompt="[order-rollback] Apply the approved rollback.",
    model=SONNET,
    tools=[read_tool("rollback_fulfillment_config")],
    output_schema=SCHEMA,
)

incident_agent = AgentDefinition(
    name="diagnoser",
    system_prompt="[incident-diagnosis] You diagnose incidents. Diagnostic tools only, no mutating actions.",
    model=SONNET,
    tools=[read_tool("get_service_health"), read_tool("get_recent_deployments"),
           read_tool("get_logs"), read_tool("open_incident_ticket")],
    output_schema=SCHEMA,
)


def register_handlers(worker: BoundFlowWorker) -> None:
    @worker.workflow("ticket-triage", version=1)
    async def _triage(ctx):
        await ctx.run_agent(triage_agent)
        return AwaitApproval(
            on_approve=Next("send_response", ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=120,
            justification="Send suggested response to the customer?",
        )

    @worker.operation("ticket-triage", "send_response")
    async def _send(ctx):
        print("      → suggest_response sent to customer.")
        return Complete()

    @worker.workflow("order-remediation", version=1)
    async def _order_v1(ctx):
        print(f"      → order-remediation running (version v{ctx.workflow_version}).")
        result = await ctx.run_agent(analyze_agent)
        print(f"      → analyst model used: {result.model_used}")
        if state.mode == "failing":
            print("      → remediation could not resolve the issue — marking run failed.")
            ctx.mark_failed()
            return Complete()
        if state.skip_approval:
            return Complete()
        return AwaitApproval(
            on_approve=Next("apply_rollback", ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=120,
            justification="Roll back fulfillment config to last-known-good?",
        )

    @worker.workflow("order-remediation", version=2)
    async def _order_v2(ctx):
        print(f"      → order-remediation running (version v{ctx.workflow_version}, smart_retry_v2).")
        result = await ctx.run_agent(analyze_v2_agent)
        print(f"      → analyst model used: {result.model_used}")
        return Complete()

    @worker.operation("order-remediation", "apply_rollback")
    async def _rollback(ctx):
        await ctx.run_agent(rollback_agent)
        print("      → rollback_fulfillment_config applied.")
        return Complete()

    @worker.workflow("incident-diagnosis", version=1)
    async def _incident(ctx):
        await ctx.run_agent(incident_agent)
        print("      → open_incident_ticket created.")
        return Complete()

    @worker.on_approval_requested
    async def _approval(req):
        approvals[req.workflow_id] = req
        print(f"      ⏳ approval requested [{req.operation_name}]: {req.justification}")


async def main() -> None:
    mock = MockLlmClient(script)
    worker = BoundFlowWorker(WORKER_ADDR, mock)
    register_handlers(worker)
    worker_task = asyncio.create_task(worker.run())

    async with ControlPlaneClient(SERVER_ADDR) as cp:
        async def register(type_: str, tenant: str, version: int):
            wf = await cp.create_workflow(type_, tenant, WorkflowConfig(version=version))
            await cp.activate_workflow(wf.id)
            return wf

        # ── STEP 1 — Register workflows ──────────────────────────────────────
        await section("STEP 1 — Register workflows")
        support = (await cp.create_tenant("support")).id
        ops = (await cp.create_tenant("operations")).id
        platform = (await cp.create_tenant("platform")).id

        triage = await register("ticket-triage", support, 1)
        order = await register("order-remediation", ops, 1)
        incident = await register("incident-diagnosis", platform, 1)

        # Runtime governance policies (opaque to the server; enforced SDK-side).
        await cp.set_agent_runtime_policy(triage.id, "triage", RuntimePolicy(max_llm_calls=1))
        await cp.set_agent_runtime_policy(order.id, "analyst", RuntimePolicy(
            max_llm_calls=2,
            tool_call_limits=[ToolCallLimit(tool="rollback_fulfillment_config", max_calls=1)],
        ))

        print()
        print("  workflow             owner       version  state")
        print("  ───────────────────────────────────────────────")
        print(f"  ticket-triage        support     v1       {await cp.get_workflow_state(triage.id)}")
        print(f"  order-remediation    operations  v1       {await cp.get_workflow_state(order.id)}")
        print(f"  incident-diagnosis   platform    v1       {await cp.get_workflow_state(incident.id)}")

        # ── STEP 2 — Trigger normal runs ─────────────────────────────────────
        await section("STEP 2 — Trigger normal runs")
        state.mode = "normal"

        print("  ticket-triage:")
        await invoke(cp, triage.id)
        await prompt_approval(cp, triage.id)
        await wait_active(cp, triage.id)

        await pause()

        state.skip_approval = True
        print("  order-remediation (runs analyst, no rollback needed):")
        await invoke(cp, order.id)
        await wait_active(cp, order.id)
        state.skip_approval = False

        await pause()

        print("  incident-diagnosis (diagnostic only):")
        await invoke(cp, incident.id)
        await wait_active(cp, incident.id)

        # ── STEP 3 — Loop detection escalates the model ──────────────────────
        await section("STEP 3 — Loop detection escalates model")
        state.skip_approval = True
        print("  Setting agent lifecycle policy: if retry_fulfillment_job called ≥ 3 times last run → escalate to Opus.")
        loop_detect = await register("order-remediation", ops, 1)
        await cp.set_agent_lifecycle_policy(loop_detect.id, "analyst", [
            AgentRule(metric=AgentMetric.CALLS_PER_TOOL, tool="retry_fulfillment_job",
                      op=Op.GTE, threshold=3, window=1, action=SetModel(value=OPUS)),
        ])
        print(f"  Simulating analyst stuck in a retry loop (expected model this run: {SONNET}).")
        state.looping = True
        await invoke(cp, loop_detect.id)
        await wait_active(cp, loop_detect.id)

        await pause("↑ loop recorded — next run will fire the escalation policy.")

        print("  Next run — lifecycle policy should have escalated the model:")
        state.looping = False
        await invoke(cp, loop_detect.id)
        await wait_active(cp, loop_detect.id)
        print(f"  (expected model this run: {OPUS})")

        # ── STEP 4/5 — Cost governance downgrades the model ──────────────────
        await section("STEP 4/5 — Cost governance + agent lifecycle policy")
        state.skip_approval = True
        print("  Setting agent lifecycle policy: if cost ≥ $1 last run → switch analyst to Haiku.")
        await cp.set_agent_lifecycle_policy(order.id, "analyst", [
            AgentRule(metric=AgentMetric.COST_USD, op=Op.GTE, threshold=1, window=1,
                      action=SetModel(value=HAIKU)),
        ])
        print(f"  Simulating order-remediation cost spike (expected model this run: {SONNET}).")
        state.cost_spike = True
        await invoke(cp, order.id)
        await wait_active(cp, order.id)

        await pause("↑ cost spike recorded — next run will fire the downgrade policy.")

        print("  Next run — lifecycle policy should switch the model:")
        state.cost_spike = False
        await invoke(cp, order.id)
        await wait_active(cp, order.id)
        print(f"  (expected model this run: {HAIKU})")

        # ── STEP 6/7 — Workflow lifecycle policy reacts to a bad v2 ───────────
        await section("STEP 6/7 — Workflow lifecycle policy reacts to a bad v2")
        state.skip_approval = False

        # 7a — repeated failures → cooldown.
        print("  (a) failures → COOLDOWN")
        wf_cooldown = await register("order-remediation", ops, 1)
        await cp.set_workflow_lifecycle_policy(wf_cooldown.id, [
            WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=1,
                         action=Cooldown(window=1, seconds=15)),
        ])
        state.mode = "failing"
        await invoke(cp, wf_cooldown.id)
        await wait_active_or(cp, wf_cooldown.id, WorkflowState.COOLDOWN)
        print(f"      → order-remediation state: {await cp.get_workflow_state(wf_cooldown.id)}")

        await pause()

        # 7b — approval rejections → pause.
        print("  (b) approval rejections → PAUSE")
        wf_pause = await register("order-remediation", ops, 1)
        await cp.set_workflow_lifecycle_policy(wf_pause.id, [
            WorkflowRule(metric=WorkflowMetric.APPROVAL_REJECTIONS, threshold=1,
                         action=Pause(window=1)),
        ])
        state.mode = "rejecting"
        await invoke(cp, wf_pause.id)
        print("      (press n to reject and trigger the pause policy)")
        await prompt_approval(cp, wf_pause.id)
        await wait_state(cp, wf_pause.id, WorkflowState.PAUSED)
        print(f"      → order-remediation state: {await cp.get_workflow_state(wf_pause.id)}")

        await pause()

        # 7c — v2's new tool fails → ToolFailureRate → roll back v2 → v1.
        print("  (c) v2 ships new smart_retry_v2 tool — it's flaky → ToolFailureRate → ROLLBACK v2 → v1")
        wf_version = await register("order-remediation", ops, 2)
        await cp.set_workflow_lifecycle_policy(wf_version.id, [
            WorkflowRule(metric=WorkflowMetric.TOOL_FAILURE_RATE, threshold=1,
                         tool="smart_retry_v2", action=SetVersion(target=1)),
        ])
        state.mode = "normal"
        print("      → invoking v2 (smart_retry_v2 will fail, triggering tool-failure rollback)...")
        await invoke(cp, wf_version.id)
        await wait_active(cp, wf_version.id)
        state.skip_approval = True
        print("      → next invoke should run v1 (rolled back to retry_fulfillment_job):")
        await invoke(cp, wf_version.id)
        await wait_active(cp, wf_version.id)

        await section("Demo complete")

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


# ── Driver helpers (mirror the C# Invoke / WaitActive / PromptApproval) ───────


async def invoke(cp, workflow_id: str) -> None:
    await cp.invoke_workflow(workflow_id, operation_timeout_seconds=60)


async def wait_lifecycle(cp, workflow_id: str, target: LifecycleState) -> None:
    while await cp.get_workflow_lifecycle_state(workflow_id) != target:
        await asyncio.sleep(0.5)


async def wait_active(cp, workflow_id: str) -> None:
    await wait_lifecycle(cp, workflow_id, LifecycleState.ACTIVE)


async def wait_state(cp, workflow_id: str, target: WorkflowState) -> None:
    while await cp.get_workflow_state(workflow_id) != target:
        await asyncio.sleep(0.5)


async def wait_active_or(cp, workflow_id: str, alt: WorkflowState) -> None:
    """Wait until the run is no longer Invoking; settle on Active or `alt`."""
    while True:
        await asyncio.sleep(0.5)
        life = await cp.get_workflow_lifecycle_state(workflow_id)
        if life == LifecycleState.INVOKING:
            continue
        wf = await cp.get_workflow_state(workflow_id)
        if wf == alt or life == LifecycleState.ACTIVE:
            return


async def wait_for_approval(cp, workflow_id: str):
    await wait_lifecycle(cp, workflow_id, LifecycleState.AWAITING_APPROVAL)
    while workflow_id not in approvals:
        await asyncio.sleep(0.2)
    return approvals.pop(workflow_id)


async def prompt_approval(cp, workflow_id: str) -> None:
    req = await wait_for_approval(cp, workflow_id)
    if AUTO:
        decision = "n" if state.mode == "rejecting" else "y"
    else:
        print(f"      [y/n] {req.justification} → ", end="", flush=True)
        decision = ""
        while decision not in ("y", "n"):
            decision = (await read_key()).lower()
    if decision == "y":
        print("approved ✔" if not AUTO else "      ✔ approved")
        await cp.approve_workflow(workflow_id, req.approval_id)
    else:
        print("rejected ✘" if not AUTO else "      ✘ rejected")
        await cp.reject_workflow(workflow_id, req.approval_id)


async def section(title: str) -> None:
    if not AUTO:
        print("  [any key to continue] ", end="", flush=True)
        await read_key()
        print("\033[2J\033[H", end="")  # clear screen + home
    print(f"══ {title} ".ljust(70, "═"))
    print()


async def pause(hint: str = "") -> None:
    if hint:
        print(f"  {hint}")
    if AUTO:
        return
    print("  [any key to continue] ", end="", flush=True)
    await read_key()
    print()


def _read_key_sync() -> str:
    if not sys.stdin.isatty():
        return sys.stdin.read(1) or " "
    if sys.platform == "win32":
        return msvcrt.getwch()
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        return sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


async def read_key() -> str:
    return await asyncio.get_event_loop().run_in_executor(None, _read_key_sync)


if __name__ == "__main__":
    asyncio.run(main())
