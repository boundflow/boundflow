"""Bound which tools a harness may use at all.

Almost everything you'd want to do to a harness's tools, the harness already does better:

  * per-action allow / deny / interrupt      → deepagents `permissions=`
  * per-tool call counting and caps          → `ToolCallLimitMiddleware`
  * watching calls and how they ended        → LangChain callbacks, see
                                               `harness_callbacks.py`

Reimplementing any of those would put BoundFlow on the wrong side of the seam. What is
left over is one thing deepagents has no notion of: a **default-deny allowlist**.
`permissions=` covers filesystem actions only, and `interrupt_on` can pause a call but
not refuse it — so "this agent may use exactly these tools and nothing else" has nowhere
else to live. That, and only that, is what this middleware is for.

`harness_call_limits` sits here too, and builds the *harness's* limiter from BoundFlow
policy: the policy stays ours, versioned and central; the counting is the harness's.

    agent = create_deep_agent(
        model=m, tools=t,
        middleware=[tool_allowlist_middleware(governor, {"ls", "read_file"}),
                    *harness_call_limits(governor)])

Ordering matters: LangChain composes "first in list as outermost layer", so this belongs
first if it is to see everything.

Two limits worth knowing before leaning on it. It doesn't reach subagents — deepagents
compiles those with their own middleware list, so a `task` subagent is bounded by its own
spec, not this one (metering still reaches them, via callbacks). And caps and allowlists
name *tools*, while a harness ships several tools per capability: capping `write_file` at
one and watching the agent reach for `edit_file` isn't a bug in the cap, it's what naming
tools instead of capabilities buys you. Enumerate the whole capability.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def tool_allowlist_middleware(governor, allowed_tools: set[str]):
    """Refuse any tool outside `allowed_tools`.

    Tools BoundFlow declared are always allowed — the customer named them by handing
    them over. Everything else the harness injected must be listed.

    A factory rather than a class so importing `boundflow` doesn't require langchain —
    the dependency is only paid by callers who actually run a harness.
    """
    from langchain.agents.middleware import AgentMiddleware
    from langchain_core.messages import ToolMessage

    def refusal(request):
        """A `ToolMessage` refusing the call, or None to let it through.

        Refuse rather than raise: the model is told and adapts, which is what
        `run_agent` already does when a declared tool's cap is spent. A refused call
        never ran, so it is never metered.
        """
        name = _tool_name(request)
        if name in allowed_tools or governor.is_governed_tool(name):
            return None
        log.debug("tool not in allowlist, refusing: tool=%s", name)
        return ToolMessage(
            content=(f"Tool '{name}' is not permitted for this agent. "
                     "Do not call it again."),
            tool_call_id=_call_id(request), status="error")

    class ToolAllowlistMiddleware(AgentMiddleware):
        """Holds the harness to a fixed set of tools."""

        name = "boundflow_tool_allowlist"

        async def awrap_tool_call(self, request, handler):
            return refusal(request) or await handler(request)

        def wrap_tool_call(self, request, handler):
            return refusal(request) or handler(request)

    return ToolAllowlistMiddleware()


def harness_call_limits(governor) -> list:
    """Translate BoundFlow's `tool_call_limits` into the harness's own limiter.

    One `ToolCallLimitMiddleware` per capped tool, using `run_limit` — a BoundFlow
    runtime policy bounds one agent run, and one graph invocation is one run. (A budget
    spanning every resume of a durable task is `thread_limit` instead; that is a
    different policy, not the one we have.)

    `exit_behavior="continue"` refuses the over-cap call and lets the agent keep going,
    matching how a declared tool's spent cap behaves.
    """
    from langchain.agents.middleware import ToolCallLimitMiddleware

    return [ToolCallLimitMiddleware(tool_name=tool, run_limit=cap,
                                    exit_behavior="continue")
            for tool, cap in governor.tool_call_caps().items()]


def _tool_name(request) -> str:
    """The tool's name, however this LangChain version exposes it."""
    call = getattr(request, "tool_call", None) or getattr(request, "call", None) or {}
    if isinstance(call, dict) and call.get("name"):
        return call["name"]
    tool = getattr(request, "tool", None)
    return getattr(tool, "name", "") or ""


def _call_id(request) -> str:
    call = getattr(request, "tool_call", None) or getattr(request, "call", None) or {}
    return call.get("id", "") if isinstance(call, dict) else ""
