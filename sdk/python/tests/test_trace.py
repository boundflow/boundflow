"""Run-trace capture. The emitted unit is an OperationTrace (one worker dispatch):
its outcome + failure flag, and the agent runs inside it, each with ordered LLM +
tool spans. All operations of one invocation share trace_id (= the run id).
"""
from __future__ import annotations

from boundflow import (
    AgentDefinition,
    BoundFlowWorker,
    Complete,
    MockContext,
    MockLlmClient,
    RuntimePolicy,
    Tool,
    Turn,
    WorkflowConfig,
    submit,
    turn,
)
from boundflow.trace import OperationTrace

from .conftest import WORKER_ADDRESS, create_isolated_tenant, run_worker, wait_for_completion

AGENT = "tracer"


class CapturingSink:
    def __init__(self) -> None:
        self.traces: list[OperationTrace] = []

    async def emit(self, trace: OperationTrace) -> None:
        self.traces.append(trace)


async def test_trace_captures_operation_with_llm_and_tool_spans(cp):
    def mock_fn(ctx: MockContext) -> Turn:
        return turn(100, 50, "search") if ctx.turn_index == 0 else submit()

    async def search_handler(_args):
        return {"results": ["a", "b"]}

    sink = CapturingSink()
    worker = BoundFlowWorker(WORKER_ADDRESS, MockLlmClient(mock_fn), trace_sink=sink)

    @worker.workflow("trace_wf", version=1)
    async def _entry(ctx):
        await ctx.run_agent(AgentDefinition(
            name=AGENT, system_prompt="trace agent", model="mock-model",
            tools=[Tool("search", "search", search_handler)],
            output_schema={"done": {"type": "boolean"}},
        ))
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "trace")
        wf = await cp.create_workflow("trace_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_runtime_policy(wf.id, AGENT, RuntimePolicy(max_llm_calls=8))
            await cp.activate_workflow(wf.id)
            request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, wf.id)

            assert len(sink.traces) == 1, f"expected one operation trace, got {len(sink.traces)}"
            t = sink.traces[0]

            # Operation-level identity + outcome. trace_id == the run id invoke returned.
            assert t.trace_id == request_id
            assert t.workflow_id == wf.id
            assert t.workflow_type == "trace_wf"
            assert t.operation == "invoke_entry"
            assert t.outcome == "completed"
            assert t.failed is False

            # One agent run nested inside.
            assert len(t.agent_runs) == 1
            run = t.agent_runs[0]
            assert run.agent == AGENT
            assert run.llm_calls == 2

            llm = [s for s in run.spans if s.kind == "llm"]
            tools = [s for s in run.spans if s.kind == "tool"]
            assert len(llm) == 2, "the tool turn + the submit turn"
            assert len(tools) == 1

            ts = tools[0]
            assert ts.name == "search"
            assert ts.output == {"results": ["a", "b"]}
            assert ts.error is None

            first = llm[0]
            assert first.attributes["gen_ai.request.model"] == "mock-model"
            assert first.attributes["gen_ai.system"] == "unknown"  # mock-model -> unknown provider
            assert first.attributes["gen_ai.usage.input_tokens"] == 100
            assert first.attributes["gen_ai.usage.output_tokens"] == 50
            # Content captured in the canonical GenAI message shape: [{role, parts}].
            roles = [m["role"] for m in first.input]
            assert "system" in roles and "user" in roles, "prompt captured as gen_ai messages"
            assert first.output[0]["role"] == "assistant"
            assert first.output[0]["parts"], "response captured as content parts"
        finally:
            await cp.delete_workflow(wf.id)


async def test_trace_captures_tool_error(cp):
    def mock_fn(ctx: MockContext) -> Turn:
        return turn(10, 5, "boom") if ctx.turn_index == 0 else submit()

    async def boom(_args):
        raise ValueError("kaboom")

    sink = CapturingSink()
    worker = BoundFlowWorker(WORKER_ADDRESS, MockLlmClient(mock_fn), trace_sink=sink)

    @worker.workflow("trace_err_wf", version=1)
    async def _entry(ctx):
        await ctx.run_agent(AgentDefinition(
            name=AGENT, system_prompt="trace agent", model="mock-model",
            tools=[Tool("boom", "boom", boom)],
            output_schema={"done": {"type": "boolean"}},
        ))
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "trace-err")
        wf = await cp.create_workflow("trace_err_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_runtime_policy(wf.id, AGENT, RuntimePolicy(max_llm_calls=8))
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, wf.id)

            assert len(sink.traces) == 1
            run = sink.traces[0].agent_runs[0]
            tool_spans = [s for s in run.spans if s.kind == "tool"]
            assert len(tool_spans) == 1
            assert tool_spans[0].name == "boom"
            assert tool_spans[0].error == "kaboom", "the tool exception is captured on the span"
            assert tool_spans[0].output is None
        finally:
            await cp.delete_workflow(wf.id)


async def test_trace_captures_operation_failure_with_no_agent(cp):
    # The operation-level capture's payoff: a run that calls no agent and marks
    # itself failed is STILL traced — failure + outcome live at the operation level.
    sink = CapturingSink()
    worker = BoundFlowWorker(WORKER_ADDRESS, MockLlmClient(lambda _: submit()), trace_sink=sink)

    @worker.workflow("trace_fail_wf", version=1)
    async def _entry(ctx):
        ctx.mark_failed()
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "trace-fail")
        wf = await cp.create_workflow("trace_fail_wf", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, wf.id)

            assert len(sink.traces) == 1
            t = sink.traces[0]
            assert t.failed is True, "ctx.mark_failed() is captured at the operation level"
            assert t.outcome == "completed"
            assert t.agent_runs == [], "no agent ran, but the operation is still traced"
        finally:
            await cp.delete_workflow(wf.id)
