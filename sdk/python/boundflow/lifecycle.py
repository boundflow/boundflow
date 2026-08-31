"""Pure policy-evaluation functions — port of LifecyclePolicyEvaluator.

These operate on the agent_state JSON the server hands back in the operation
context. Agent runtime + lifecycle policies are opaque to the server (stored as
jsonb, enforced here); only the metric snapshots use fixed proto field names
(cost_usd, llm_calls, tokens_used, calls_per_tool, ran_at).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pydantic import ValidationError

from .policies import (
    AgentMetric,
    AgentRule,
    Op,
    RuntimePolicy,
    SetMaxCostUsd,
    SetMaxLlmCalls,
    SetMaxTokensPerCall,
    SetModel,
    ToolCallLimit,
    ToolFailureLimit,
)

log = logging.getLogger("boundflow.lifecycle")


@dataclass
class InvocationSnapshot:
    tokens_used: int = 0
    cost_usd: float = 0.0
    llm_calls: int = 0
    calls_per_tool: dict[str, int] | None = None
    ran_at: int = 0


# ── Loading from the operation-context agent_state ────────────────────────────


def load_runtime_policy(node: dict | None) -> RuntimePolicy:
    """Parse the runtime-policy JSON the server hands back.

    Validates the node as a whole rather than field by field. It used to be the latter,
    which meant a policy field the customer set travelled the whole way here and was then
    silently dropped for not being on the list — the failure mode being that a limit you
    wrote is simply not enforced, with nothing to see anywhere.

    Individual malformed entries are dropped instead of failing the operation, since the
    server stores this opaquely and never validates it. Each one is logged: a rule that
    isn't there is exactly what you don't want to discover from an audit later.
    """
    if not node:
        return RuntimePolicy()
    try:
        return RuntimePolicy.model_validate(node)
    except ValidationError:
        pass
    kept = {k: v for k, v in node.items() if not isinstance(v, list)}
    for field, entries in ((k, v) for k, v in node.items() if isinstance(v, list)):
        good = []
        for entry in entries:
            try:
                RuntimePolicy.model_validate({**kept, field: [*good, entry]})
            except ValidationError:
                log.warning("dropping malformed %s entry from runtime policy: %r",
                            field, entry)
                continue
            good.append(entry)
        kept[field] = good
    try:
        return RuntimePolicy.model_validate(kept)
    except ValidationError:
        # Something scalar is wrong (a string where a number belongs), and there's no
        # entry to drop. Refusing to guess: an unenforceable policy is not an empty one.
        log.error("unusable runtime policy, failing the operation: %r", node)
        raise


def load_lifecycle_rules(state_node: dict | None) -> list[AgentRule]:
    if not state_node:
        return []
    rules = (state_node.get("lifecycle_policy") or {}).get("rules")
    if not rules:
        return []
    return [AgentRule.model_validate(r) for r in rules]


def load_history(state_node: dict | None) -> list[InvocationSnapshot]:
    if not state_node:
        return []
    return [
        InvocationSnapshot(
            tokens_used=e.get("tokens_used", 0),
            cost_usd=e.get("cost_usd", 0.0),
            llm_calls=e.get("llm_calls", 0),
            calls_per_tool=e.get("calls_per_tool") or {},
            ran_at=e.get("ran_at", 0),
        )
        for e in (state_node.get("invocation_metrics") or [])
    ]


# ── Evaluation ────────────────────────────────────────────────────────────────


def apply_lifecycle_rules(
    rules: list[AgentRule],
    history: list[InvocationSnapshot],
    current: RuntimePolicy,
) -> tuple[RuntimePolicy, list[tuple[AgentRule, float]]]:
    """Evaluate each rule over its window; apply the action when it fires. Returns
    the resulting policy plus the rules that fired, each with the metric value that
    crossed (for the agent-lifecycle audit).

    Multiple firing rules compose left-to-right.
    """
    fired: list[tuple[AgentRule, float]] = []
    for rule in rules:
        window = history[-rule.window:] if rule.window > 0 else history
        total = sum(metric_value(e, rule.metric, rule.tool) for e in window)
        if not _evaluate(total, rule.op, rule.threshold):
            continue
        current = _apply_action(current, rule.action)
        fired.append((rule, total))
    return current, fired


def metric_value(entry: InvocationSnapshot, metric: AgentMetric, tool: str | None) -> float:
    if metric is AgentMetric.TOKENS_USED:
        return entry.tokens_used
    if metric is AgentMetric.COST_USD:
        return entry.cost_usd
    if metric is AgentMetric.LLM_CALLS:
        return entry.llm_calls
    if metric is AgentMetric.CALLS_PER_TOOL:
        cpt = entry.calls_per_tool or {}
        if tool is not None:
            return cpt.get(tool, 0)
        return max(cpt.values(), default=0)
    return 0


def _evaluate(total: float, op: Op, threshold: float) -> bool:
    return {
        Op.LT: total < threshold,
        Op.LTE: total <= threshold,
        Op.GT: total > threshold,
        Op.GTE: total >= threshold,
        Op.EQ: total == threshold,
    }[op]


def _apply_action(policy: RuntimePolicy, action) -> RuntimePolicy:
    if isinstance(action, SetModel):
        return policy.model_copy(update={"model": action.value or policy.model})
    if isinstance(action, SetMaxLlmCalls):
        return policy.model_copy(update={"max_llm_calls": action.value})
    if isinstance(action, SetMaxCostUsd):
        return policy.model_copy(update={"max_cost_usd": action.value})
    if isinstance(action, SetMaxTokensPerCall):
        return policy.model_copy(update={"max_tokens_per_call": action.value})
    return policy
