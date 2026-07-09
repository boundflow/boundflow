"""gRPC transport — worker bidi stream + proto⇄domain conversion.

Port of BoundFlow.SDK WorkerClient (the stream loop) and the MapToProto helpers
in BoundFlowWorker, on top of grpc.aio. The server drives the session with
Launch/Cancel commands; the worker acks IN_PROGRESS, runs the operation off the
receive loop, then reports the result and re-arms with ReadyForWork.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Awaitable, Callable

import grpc

log = logging.getLogger("boundflow.worker")
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from boundflow.v1 import agent_policy_pb2 as ap_pb
from boundflow.v1 import operation_pb2 as op_pb
from boundflow.v1 import worker_pb2 as wk_pb
from boundflow.v1 import worker_pb2_grpc as wk_grpc

# dispatch: given the launched operation proto, produce the result proto.
Dispatch = Callable[[op_pb.AtomicOperation], Awaitable[op_pb.AtomicOperationResult]]


def _strip_scheme(addr: str) -> str:
    return addr.split("://", 1)[1] if "://" in addr else addr


def context_to_dict(op: op_pb.AtomicOperation) -> dict:
    """AtomicOperation.context (Struct) → plain dict, camelCase top-level keys
    (matching the .NET JsonFormatter), opaque policy contents preserved."""
    if not op.HasField("context"):
        return {}
    return json_format.MessageToDict(op.context)


def dict_to_struct(d: dict) -> Struct:
    s = Struct()
    s.update(d or {})
    return s


def new_approval_id() -> str:
    return str(uuid.uuid4())


def metrics_to_proto(snapshot: dict) -> op_pb.AgentInvocationMetrics:
    m = op_pb.AgentInvocationMetrics(
        cost_usd=snapshot.get("cost_usd", 0.0),
        llm_calls=snapshot.get("llm_calls", 0),
        tokens_used=snapshot.get("tokens_used", 0),
        latency_seconds=snapshot.get("latency_seconds", 0.0),
        ran_at=snapshot.get("ran_at", 0),
    )
    for tool, count in (snapshot.get("calls_per_tool") or {}).items():
        m.calls_per_tool[tool] = count
    for tool, count in (snapshot.get("tool_failure_counts") or {}).items():
        m.tool_failure_counts[tool] = count
    return m


_AGENT_METRIC_PB = {
    "tokens_used": ap_pb.AGENT_METRIC_TOKENS_USED,
    "cost_usd": ap_pb.AGENT_METRIC_COST_USD,
    "llm_calls": ap_pb.AGENT_METRIC_LLM_CALLS,
    "calls_per_tool": ap_pb.AGENT_METRIC_CALLS_PER_TOOL,
}
_AGENT_OP_PB = {
    "less_than": ap_pb.AGENT_OP_LT,
    "less_than_or_equal": ap_pb.AGENT_OP_LTE,
    "greater_than": ap_pb.AGENT_OP_GT,
    "greater_than_or_equal": ap_pb.AGENT_OP_GTE,
    "equal": ap_pb.AGENT_OP_EQ,
}


def _enum_value(v) -> str:
    return getattr(v, "value", v)


def _runtime_policy_to_proto(p) -> ap_pb.AgentRuntimePolicy:
    return ap_pb.AgentRuntimePolicy(
        model=p.model or "",
        max_llm_calls=p.max_llm_calls,
        max_cost_usd=p.max_cost_usd,
        max_tokens_per_call=p.max_tokens_per_call,
        max_call_seconds=p.max_call_seconds,
        tool_call_limits=[ap_pb.ToolCallLimit(tool=l.tool, max_calls=l.max_calls) for l in p.tool_call_limits],
    )


def _agent_action_to_proto(action) -> ap_pb.AgentRuleAction:
    field = action.field
    if field == "model":
        return ap_pb.AgentRuleAction(field=ap_pb.AGENT_RULE_ACTION_SET_MODEL, model=action.value)
    if field == "max_llm_calls":
        return ap_pb.AgentRuleAction(field=ap_pb.AGENT_RULE_ACTION_SET_MAX_LLM_CALLS, max_llm_calls=action.value)
    if field == "max_cost_usd":
        return ap_pb.AgentRuleAction(field=ap_pb.AGENT_RULE_ACTION_SET_MAX_COST_USD, max_cost_usd=action.value)
    if field == "max_tokens_per_call":
        return ap_pb.AgentRuleAction(field=ap_pb.AGENT_RULE_ACTION_SET_MAX_TOKENS_PER_CALL, max_tokens_per_call=action.value)
    return ap_pb.AgentRuleAction()


def _agent_rule_to_proto(rule) -> ap_pb.AgentRule:
    return ap_pb.AgentRule(
        metric=_AGENT_METRIC_PB.get(_enum_value(rule.metric), ap_pb.AGENT_METRIC_UNSPECIFIED),
        op=_AGENT_OP_PB.get(_enum_value(rule.op), ap_pb.AGENT_OP_UNSPECIFIED),
        threshold=rule.threshold,
        window=rule.window,
        tool=rule.tool or "",
        action=_agent_action_to_proto(rule.action),
    )


def agent_policy_action_to_proto(action: dict) -> ap_pb.AgentPolicyAction:
    """Map the SDK-side agent policy action ({base_policy, effective_policy,
    fired_rules:[(rule, value)]}) to the typed proto for the operation result."""
    return ap_pb.AgentPolicyAction(
        base_policy=_runtime_policy_to_proto(action["base_policy"]),
        effective_policy=_runtime_policy_to_proto(action["effective_policy"]),
        fired_rules=[
            ap_pb.FiredAgentRule(rule=_agent_rule_to_proto(rule), trigger_value=float(value))
            for (rule, value) in action["fired_rules"]
        ],
    )


class WorkerSession:
    """Owns the bidi stream and dispatch loop. One operation in flight at a time."""

    def __init__(self, address: str, api_key: str, capabilities: list[tuple[str, int]] | None = None) -> None:
        self._target = _strip_scheme(address)
        self._secure = address.startswith("https://")
        self._session_id = str(uuid.uuid4())
        self._write_lock = asyncio.Lock()
        self._metadata = (("x-api-key", api_key),)
        self._capabilities = [
            wk_pb.WorkerCapability(workflow_type=rt, workflow_version=v)
            for rt, v in (capabilities or [])
        ]

    async def run(self, dispatch: Dispatch) -> None:
        if self._secure:
            channel_ctx = grpc.aio.secure_channel(self._target, grpc.ssl_channel_credentials())
        else:
            channel_ctx = grpc.aio.insecure_channel(self._target)
        async with channel_ctx as channel:
            stub = wk_grpc.WorkerServiceStub(channel)
            call = stub.WorkerSession(metadata=self._metadata)
            await self._write(call, self._ready())

            op_task: asyncio.Task | None = None
            op_id: str | None = None

            async for command in call:
                which = command.WhichOneof("payload")
                if which == "launch":
                    op = command.launch.operation
                    op_id = op.id
                    log.debug("launch: op_id=%s workflow_type=%s name=%s version=%d",
                              op.id, op.workflow_type, op.name, op.workflow_version)
                    # Ack IN_PROGRESS before starting; keep the receive loop free.
                    await self._write(call, self._update(op.id, op_pb.OPERATION_STATUS_IN_PROGRESS))
                    op_task = asyncio.create_task(self._run_operation(call, op, dispatch))
                elif which == "cancel":
                    if op_task is not None and command.cancel.operation_id == op_id:
                        op_task.cancel()
                        try:
                            await op_task
                        except asyncio.CancelledError:
                            await self._write(call, self._update(op_id, op_pb.OPERATION_STATUS_CANCELLED))
                    op_task, op_id = None, None
                    await self._write(call, self._ready())

    async def _run_operation(self, call, op: op_pb.AtomicOperation, dispatch: Dispatch) -> None:
        try:
            result = await dispatch(op)
        except asyncio.CancelledError:
            raise  # surfaced to the main loop, which sends CANCELLED
        except Exception as ex:  # noqa: BLE001 — report a handler failure
            log.error("operation FAILED: op_id=%s error=%s", op.id, ex, exc_info=True)
            await self._write(call, self._update(op.id, op_pb.OPERATION_STATUS_FAILED, str(ex)))
            await self._write(call, self._ready())
            return
        await self._write(call, wk_pb.WorkerMessage(
            session_id=self._session_id,
            update=wk_pb.OperationUpdate(operation_id=op.id, result=result),
        ))
        await self._write(call, self._ready())

    async def _write(self, call, msg: wk_pb.WorkerMessage) -> None:
        async with self._write_lock:
            await call.write(msg)

    def _ready(self) -> wk_pb.WorkerMessage:
        return wk_pb.WorkerMessage(
            session_id=self._session_id,
            ready=wk_pb.ReadyForWork(capabilities=self._capabilities),
        )

    def _update(self, operation_id: str, status, message: str = "") -> wk_pb.WorkerMessage:
        return wk_pb.WorkerMessage(
            session_id=self._session_id,
            update=wk_pb.OperationUpdate(
                operation_id=operation_id,
                result=op_pb.AtomicOperationResult(status=status, message=message),
            ),
        )
