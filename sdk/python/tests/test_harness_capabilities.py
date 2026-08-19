"""Capability-level caps and allowlists for a harness's own tools.

The behaviour these pin down is the one that made per-tool caps look adequate and then
wasn't: cap `write_file`, and the agent uses `edit_file`.
"""
from __future__ import annotations

import asyncio

import pytest

from boundflow.capabilities import (
    capability_of, file_permissions, register_capability, tools_with)
from boundflow.harness_middleware import (
    harness_call_limits, harness_middleware, tool_allowlist_middleware)
from boundflow.policies import (
    CapabilityCallLimit, FileRule, RuntimePolicy, ToolCallLimit)
from boundflow.governed import AgentGovernor


class _Request:
    """The two fields the middleware reads off a `ToolCallRequest`."""

    def __init__(self, name: str) -> None:
        self.tool_call = {"name": name, "args": {}, "id": f"call_{name}", "type": "tool_call"}


def _governor(**policy) -> AgentGovernor:
    return AgentGovernor("operator", RuntimePolicy(**policy), "claude-sonnet-4-6",
                         collect_spans=False)


def _capped(capability: str, max_calls: int) -> AgentGovernor:
    return _governor(capability_call_limits=[
        CapabilityCallLimit(capability=capability, max_calls=max_calls)])


async def _call(middleware, name: str, *, ran: list[str]):
    async def handler(request):
        ran.append(name)
        return f"{name} ok"

    return await middleware.awrap_tool_call(_Request(name), handler)


def test_capability_matches_deepagents_filesystem_vocabulary():
    # If deepagents renames an operation or files a tool differently, a customer writing
    # one rule against `permissions=` and another against BoundFlow policy would get two
    # different answers. Pin the mapping.
    assert tools_with("read") >= {"ls", "read_file", "glob", "grep"}
    assert tools_with("write") >= {"write_file", "edit_file", "delete"}
    assert capability_of("task") == "spawn"


def test_capability_cap_covers_every_tool_that_does_the_thing():
    governor = _capped("write", 1)
    [middleware] = harness_call_limits(governor)
    ran: list[str] = []

    # One write is allowed, and metering records it the way callbacks would.
    assert asyncio.run(_call(middleware, "write_file", ran=ran)) == "write_file ok"
    governor.record_harness_tool("write_file")

    # The second write is refused even though it's a *different tool* — the point of
    # the whole exercise.
    refused = asyncio.run(_call(middleware, "edit_file", ran=ran))
    assert refused.status == "error"
    assert "at most 1 'write'" in refused.content
    assert ran == ["write_file"]

    # Reads are untouched: the cap bounds a capability, not the agent.
    assert asyncio.run(_call(middleware, "read_file", ran=ran)) == "read_file ok"


def test_refused_call_is_not_metered():
    # A refusal that counted itself would push the number past its own cap forever, in
    # the same metric lifecycle rules read.
    governor = _capped("write", 0)
    [middleware] = harness_call_limits(governor)
    ran: list[str] = []
    assert asyncio.run(_call(middleware, "write_file", ran=ran)).status == "error"
    assert ran == []
    assert governor.calls_per_tool == {}


def test_tool_caps_are_delegated_to_the_harness():
    # A cap naming one tool is the harness's own middleware, not ours — we don't
    # reimplement counting it already does.
    governor = _governor(
        tool_call_limits=[ToolCallLimit(tool="write_file", max_calls=1)],
        capability_call_limits=[CapabilityCallLimit(capability="write", max_calls=3)])
    middleware = harness_call_limits(governor)
    names = [m.name for m in middleware]
    assert names[0] == "boundflow_capability_limits"  # outermost
    assert any("ToolCallLimit" in type(m).__name__ for m in middleware)


def test_allowlist_accepts_capabilities_and_declared_tools():
    governor = _governor(allowed_capabilities=["read"], allowed_tools=["task"])
    governor.register_governed_tools(["restart_database"])
    middleware = tool_allowlist_middleware(governor)
    ran: list[str] = []

    assert asyncio.run(_call(middleware, "grep", ran=ran)) == "grep ok"
    assert asyncio.run(_call(middleware, "task", ran=ran)) == "task ok"
    # Declared tools are always allowed: the customer named them by handing them over.
    assert asyncio.run(_call(middleware, "restart_database", ran=ran)) == "restart_database ok"

    refused = asyncio.run(_call(middleware, "write_file", ran=ran))
    assert refused.status == "error"
    assert "not permitted" in refused.content
    assert "write_file" not in ran


def test_register_capability_covers_a_customers_own_tool():
    register_capability("restart_database", "write")
    try:
        governor = _capped("write", 1)
        [middleware] = harness_call_limits(governor)
        ran: list[str] = []
        asyncio.run(_call(middleware, "restart_database", ran=ran))
        governor.record_harness_tool("restart_database")
        # A customer tool spends the same budget as the harness's own, which is the
        # only reading of "at most one write" that means anything.
        assert asyncio.run(_call(middleware, "write_file", ran=ran)).status == "error"
    finally:
        from boundflow.capabilities import TOOL_CAPABILITIES
        TOOL_CAPABILITIES.pop("restart_database", None)


def test_unclassified_tools_are_untouched_by_capability_caps():
    # Capabilities bound what you know about; allowlists bound what you don't.
    governor = _capped("write", 0)
    [middleware] = harness_call_limits(governor)
    ran: list[str] = []
    assert asyncio.run(_call(middleware, "send_email", ran=ran)) == "send_email ok"


def test_no_allowlist_by_default():
    # An empty allowlist means "no allowlist", not "nothing allowed" — the opposite
    # default would forbid everything the moment someone added the field.
    assert tool_allowlist_middleware(_governor()) is None
    assert harness_middleware(_governor()) == []


def test_file_rules_translate_to_deepagents_permissions():
    governor = _governor(file_rules=[
        FileRule(operations=["write"], paths=["/secrets/**"], mode="deny"),
        FileRule(operations=["read", "write"], paths=["/prod/**"], mode="interrupt")])
    permissions = file_permissions(governor.policy)
    assert [(p.operations, p.paths, p.mode) for p in permissions] == [
        (["write"], ["/secrets/**"], "deny"),
        (["read", "write"], ["/prod/**"], "interrupt")]


def test_file_rule_paths_are_validated_where_they_are_written():
    # Same constraints deepagents enforces, applied when the rule is declared rather
    # than when an agent is eventually built from it.
    for bad in ["secrets/**", "/work/../etc", "/~/keys"]:
        with pytest.raises(ValueError):
            FileRule(operations=["write"], paths=[bad])


def test_policy_round_trips_through_plain_data():
    # Runtime policy travels to the worker as an opaque JSON struct, so everything here
    # has to survive being written as YAML and parsed back.
    policy = RuntimePolicy(
        max_llm_calls=20,
        capability_call_limits=[CapabilityCallLimit(capability="write", max_calls=5)],
        allowed_capabilities=["read", "write"],
        file_rules=[FileRule(operations=["write"], paths=["/secrets/**"], mode="deny")])
    assert RuntimePolicy.model_validate(policy.model_dump()) == policy
