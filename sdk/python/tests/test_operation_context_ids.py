"""The operation's identifiers, exposed for callers that need to key durable state.

A run spans several operations, so anything that must outlive one of them — a
harness's conversation thread, a store namespace — has to key on the run rather
than the operation. These are how a handler gets at that without reaching into
private attributes.
"""
from __future__ import annotations

from boundflow.worker import OperationContext


class _Op:
    """The fields OperationContext reads off an operation."""
    name = "invoke_entry"
    workflow_version = 3
    workflow_id = "workflow-1"
    request_id = "req-1"
    workflow_type = "researcher"
    context: dict = {}


def test_ids_are_readable_without_touching_privates():
    ctx = OperationContext(_Op(), orchestrator=None)
    assert ctx.workflow_id == "workflow-1"
    assert ctx.request_id == "req-1"
    # Alongside what was already public.
    assert ctx.name == "invoke_entry"
    assert ctx.workflow_version == 3
