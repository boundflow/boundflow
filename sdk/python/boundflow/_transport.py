"""gRPC transport — worker bidi stream + proto⇄domain conversion.

Port of BoundFlow.SDK WorkerClient (the stream loop) and the MapToProto helpers
in BoundFlowWorker, on top of grpc.aio. The server drives the session with
Launch/Cancel commands; the worker acks IN_PROGRESS, runs the operation off the
receive loop, then reports the result and re-arms with ReadyForWork.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Awaitable, Callable

import grpc
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from convergeplane.v1 import operation_pb2 as op_pb
from convergeplane.v1 import worker_pb2 as wk_pb
from convergeplane.v1 import worker_pb2_grpc as wk_grpc

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
        ran_at=snapshot.get("ran_at", 0),
    )
    for tool, count in (snapshot.get("calls_per_tool") or {}).items():
        m.calls_per_tool[tool] = count
    for tool, count in (snapshot.get("tool_failure_counts") or {}).items():
        m.tool_failure_counts[tool] = count
    return m


class WorkerSession:
    """Owns the bidi stream and dispatch loop. One operation in flight at a time."""

    def __init__(self, address: str, api_key: str) -> None:
        self._target = _strip_scheme(address)
        self._session_id = str(uuid.uuid4())
        self._write_lock = asyncio.Lock()
        self._metadata = (("x-api-key", api_key),)

    async def run(self, dispatch: Dispatch) -> None:
        async with grpc.aio.insecure_channel(self._target) as channel:
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
            import traceback
            print(f"\n[BoundFlow worker] operation FAILED: {ex}", flush=True)
            traceback.print_exc()
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
        return wk_pb.WorkerMessage(session_id=self._session_id, ready=wk_pb.ReadyForWork())

    def _update(self, operation_id: str, status, message: str = "") -> wk_pb.WorkerMessage:
        return wk_pb.WorkerMessage(
            session_id=self._session_id,
            update=wk_pb.OperationUpdate(
                operation_id=operation_id,
                result=op_pb.AtomicOperationResult(status=status, message=message),
            ),
        )
