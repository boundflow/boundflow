"""run_agent(budget=...) — narrowing one agent step to what's left of a budget
that spans several steps.

BoundFlow's runtime policy caps a single agent step, so a workflow that calls
run_agent repeatedly (a loop, or several operations in one run) gets a fresh cap
each time. These cover the merge itself: it tightens, it never loosens, and a
spent budget refuses rather than silently becoming unlimited.
"""
from __future__ import annotations

import pytest

from boundflow import (
    AgentPolicyLimitExceeded, Budget, RuntimePolicy, ToolCallLimit, ToolFailureLimit,
)
from boundflow.worker import _apply_budget

AGENT = "responder"


def _apply(policy: RuntimePolicy, budget: Budget | None) -> RuntimePolicy:
    return _apply_budget(policy, budget, AGENT)


def test_no_budget_leaves_the_policy_alone():
    policy = RuntimePolicy(max_llm_calls=5, max_cost_usd=1.0)
    assert _apply(policy, None) is policy


def test_budget_narrows_a_policy_cap():
    policy = RuntimePolicy(max_llm_calls=10, max_cost_usd=1.00)
    out = _apply(policy, Budget(max_llm_calls=3, max_cost_usd=0.25))
    assert out.max_llm_calls == 3
    assert out.max_cost_usd == 0.25


def test_budget_cannot_loosen_a_policy_cap():
    """run_agent is called from workflow code, so the server-side policy has to stay
    the ceiling — otherwise a handler could opt itself out of its own governance."""
    policy = RuntimePolicy(max_llm_calls=5, max_cost_usd=0.50)
    out = _apply(policy, Budget(max_llm_calls=999, max_cost_usd=999.0))
    assert out.max_llm_calls == 5, "budget must not raise a policy cap"
    assert out.max_cost_usd == 0.50


def test_budget_applies_where_policy_set_no_cap():
    """0 means 'unset' in RuntimePolicy, so an uncapped policy takes the budget as-is
    rather than min()-ing against 0 and staying uncapped."""
    out = _apply(RuntimePolicy(), Budget(max_llm_calls=4, max_cost_usd=0.20))
    assert out.max_llm_calls == 4
    assert out.max_cost_usd == 0.20


def test_partial_budget_only_touches_what_it_names():
    policy = RuntimePolicy(max_llm_calls=8, max_cost_usd=2.0, max_tokens_per_call=500)
    out = _apply(policy, Budget(max_llm_calls=2))
    assert out.max_llm_calls == 2
    assert out.max_cost_usd == 2.0, "unnamed fields are left alone"
    assert out.max_tokens_per_call == 500


def test_the_original_policy_is_not_mutated():
    policy = RuntimePolicy(max_llm_calls=10)
    _apply(policy, Budget(max_llm_calls=1))
    assert policy.max_llm_calls == 10


@pytest.mark.parametrize("spent", [0, -1, -50])
def test_spent_call_budget_refuses_instead_of_going_unlimited(spent):
    """The landmine: a computed remaining budget lands on 0 exactly when it's spent,
    and 0 means 'unlimited' in RuntimePolicy — so passing it through would drop the
    cap at the precise moment it should bite."""
    with pytest.raises(AgentPolicyLimitExceeded, match="max_llm_calls"):
        _apply(RuntimePolicy(max_llm_calls=5), Budget(max_llm_calls=spent))


@pytest.mark.parametrize("spent", [0, -0.01])
def test_spent_cost_budget_refuses(spent):
    with pytest.raises(AgentPolicyLimitExceeded, match="max_cost_usd"):
        _apply(RuntimePolicy(max_cost_usd=1.0), Budget(max_cost_usd=spent))


def test_refusal_happens_before_any_call_is_made():
    """It raises out of the merge, so no LLM call is attempted — the point is not to
    spend anything once the budget is gone."""
    with pytest.raises(AgentPolicyLimitExceeded) as exc:
        _apply(RuntimePolicy(), Budget(max_llm_calls=0))
    assert "not making a call" in str(exc.value)
    assert AGENT in str(exc.value)


# ── per-tool budgets ─────────────────────────────────────────────────────────


def test_tool_call_budget_tightens_per_tool():
    policy = RuntimePolicy(tool_call_limits=[ToolCallLimit(tool="search", max_calls=5)])
    out = _apply(policy, Budget(tool_call_limits={"search": 2}))
    assert {l.tool: l.max_calls for l in out.tool_call_limits} == {"search": 2}


def test_tool_call_budget_cannot_loosen():
    policy = RuntimePolicy(tool_call_limits=[ToolCallLimit(tool="search", max_calls=2)])
    out = _apply(policy, Budget(tool_call_limits={"search": 99}))
    assert {l.tool: l.max_calls for l in out.tool_call_limits} == {"search": 2}


def test_tool_budget_can_cap_a_tool_policy_left_uncapped():
    out = _apply(RuntimePolicy(), Budget(tool_call_limits={"search": 3}))
    assert {l.tool: l.max_calls for l in out.tool_call_limits} == {"search": 3}


def test_tool_budget_keeps_policy_caps_it_doesnt_mention():
    policy = RuntimePolicy(tool_call_limits=[
        ToolCallLimit(tool="search", max_calls=5), ToolCallLimit(tool="write", max_calls=1)])
    out = _apply(policy, Budget(tool_call_limits={"search": 2}))
    assert {l.tool: l.max_calls for l in out.tool_call_limits} == {"search": 2, "write": 1}


def test_spent_tool_budget_blocks_that_tool_without_failing_the_step():
    """Unlike max_llm_calls, an exhausted tool doesn't stop the step — the tool is
    capped at 0 (blocked) and the agent carries on with everything else."""
    out = _apply(RuntimePolicy(), Budget(tool_call_limits={"search": 0}))
    assert {l.tool: l.max_calls for l in out.tool_call_limits} == {"search": 0}


def test_negative_tool_remaining_clamps_to_blocked():
    out = _apply(RuntimePolicy(), Budget(tool_call_limits={"search": -3}))
    assert out.tool_call_limits[0].max_calls == 0


def test_tool_failure_budget_tightens_the_same_way():
    policy = RuntimePolicy(tool_failure_limits=[ToolFailureLimit(tool="stripe", max_failures=5)])
    out = _apply(policy, Budget(tool_failure_limits={"stripe": 1}))
    assert {l.tool: l.max_failures for l in out.tool_failure_limits} == {"stripe": 1}
