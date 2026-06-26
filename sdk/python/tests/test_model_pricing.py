"""Customer-facing pricing tests.

Cost itself isn't directly readable from the control plane, so these drive
behavior with the mock LLM's scripted token counts and assert on the observable
effects of cost-based policies. All mock-based (no ANTHROPIC_API_KEY needed).
"""
from __future__ import annotations

import boundflow as bf
from boundflow import (
    AgentDefinition,
    AgentMetric,
    AgentRule,
    BoundFlowWorker,
    Complete,
    MockLlmClient,
    Op,
    RuntimePolicy,
    SetModel,
    WorkflowConfig,
    submit,
    turn,
)

from .conftest import (
    HAIKU,
    WORKER_ADDRESS,
    create_isolated_tenant,
    run_worker,
    wait_for_completion,
)

OPUS = "claude-opus-4-8"
HAIKU_ALIAS = "claude-haiku-4-5"  # pricing key the dated HAIKU id resolves to

# The seeded global defaults (per 1M tokens, USD).
DEFAULTS = {
    "claude-opus-4-8": (5.0, 25.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-haiku-4-5": (1.0, 5.0),
}


async def _reset_pricing(cp) -> None:
    """Restore overrides to the seeded defaults (overrides are tenant-group scoped
    and shared across tests, so a mutating test must clean up after itself)."""
    for model_id, (in_rate, out_rate) in DEFAULTS.items():
        await cp.set_model_pricing(model_id, in_rate, out_rate)


# ── Pricing API ──────────────────────────────────────────────────────────────


async def test_list_pricing_returns_seeded_defaults(cp):
    pricing = await cp.list_model_pricing()
    assert pricing["claude-opus-4-8"] == {"input_per_1m": 5.0, "output_per_1m": 25.0}
    assert pricing["claude-sonnet-4-6"] == {"input_per_1m": 3.0, "output_per_1m": 15.0}
    assert pricing["claude-haiku-4-5"] == {"input_per_1m": 1.0, "output_per_1m": 5.0}


async def test_set_override_merges_and_preserves_other_defaults(cp):
    try:
        await cp.set_model_pricing("claude-opus-4-8", 7.0, 35.0)
        pricing = await cp.list_model_pricing()
        assert pricing["claude-opus-4-8"] == {"input_per_1m": 7.0, "output_per_1m": 35.0}
        # Untouched models keep their defaults.
        assert pricing["claude-haiku-4-5"] == {"input_per_1m": 1.0, "output_per_1m": 5.0}
    finally:
        await _reset_pricing(cp)


# ── Cost-driven runtime policy ───────────────────────────────────────────────


def _looping_ping_worker(per_call_input_tokens: int, calls_seen: list):
    """Worker whose agent calls `ping` every turn (never submits on its own), so a
    cost/call cap is what stops the loop. Records ping invocations into calls_seen."""
    def mock_fn(_ctx):
        return turn(per_call_input_tokens, 0, "ping")

    async def ping(_):
        calls_seen.append(1)
        return "pong"

    worker = BoundFlowWorker(WORKER_ADDRESS, MockLlmClient(mock_fn))

    def agent():
        return AgentDefinition(
            name="capper",
            system_prompt="loops on ping",
            model=HAIKU,
            tools=[bf.Tool("ping", "ping", ping)],
            output_schema={"done": {"type": "boolean"}},
        )

    return worker, agent


async def _run_capped(cp, worker, agent, prefix, max_cost_usd, max_llm_calls):
    @worker.workflow("cost_cap", version=1)
    async def _entry(ctx):
        await ctx.run_agent(agent())
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, prefix)
        wf = await cp.create_workflow("cost_cap", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_runtime_policy(
                wf.id, "capper",
                RuntimePolicy(max_llm_calls=max_llm_calls, max_cost_usd=max_cost_usd))
            await cp.activate_workflow(wf.id)
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, wf.id)
        finally:
            await cp.delete_workflow(wf.id)


async def test_runtime_cost_cap_limits_calls_below_llm_call_limit(cp):
    """With Haiku at $1/1M, 100k tokens/call = $0.10. A $0.25 cost cap stops the
    loop at 3 calls — before the (deliberately higher) max_llm_calls of 5 — proving
    the *cost* cap, not the call cap, did the stopping."""
    calls: list = []
    worker, agent = _looping_ping_worker(100_000, calls)
    await _run_capped(cp, worker, agent, "cost-cap", max_cost_usd=0.25, max_llm_calls=5)
    assert len(calls) == 3, f"expected 3 calls before the $0.25 cap, got {len(calls)}"


async def test_cost_cap_uses_current_pricing_not_initial(cp):
    """Same tokens + same cap, but a pricing change between runs flips the outcome —
    proving the worker prices with the *current* rate, not a stale/initial one.

    Haiku default ($1/1M): 100k/call = $0.10 → 3 calls before the $0.25 cap.
    After overriding Haiku to $5/1M: 100k/call = $0.50 → 1 call trips the cap.
    """
    try:
        default_calls: list = []
        w1, a1 = _looping_ping_worker(100_000, default_calls)
        await _run_capped(cp, w1, a1, "price-before", max_cost_usd=0.25, max_llm_calls=5)

        await cp.set_model_pricing(HAIKU_ALIAS, 5.0, 25.0)  # 5x more expensive

        overridden_calls: list = []
        w2, a2 = _looping_ping_worker(100_000, overridden_calls)
        await _run_capped(cp, w2, a2, "price-after", max_cost_usd=0.25, max_llm_calls=5)

        assert len(default_calls) == 3, f"default pricing: expected 3, got {len(default_calls)}"
        assert len(overridden_calls) == 1, f"overridden pricing: expected 1, got {len(overridden_calls)}"
        assert len(overridden_calls) < len(default_calls)
    finally:
        await _reset_pricing(cp)


# ── Cost-driven agent lifecycle policy ───────────────────────────────────────


async def test_agent_lifecycle_cost_policy_switches_model(cp):
    """An agent-lifecycle rule 'if cost >= $1 last run -> switch to Haiku' fires off
    the cost the worker computes from pricing. Run 1 (Opus, 1M tokens = $5) records a
    high cost; run 2 should come back on Haiku."""
    models: list = []

    def mock_fn(ctx):
        # One token-heavy turn to rack up cost, then submit.
        return turn(1_000_000, 0) if ctx.turn_index == 0 else submit()

    worker = BoundFlowWorker(WORKER_ADDRESS, MockLlmClient(mock_fn))

    def agent():
        return AgentDefinition(
            name="analyst",
            system_prompt="costly analyst",
            model=OPUS,
            output_schema={"summary": {"type": "string"}},
        )

    @worker.workflow("cost_switch", version=1)
    async def _entry(ctx):
        result = await ctx.run_agent(agent())
        models.append(result.model_used)
        return Complete()

    async with run_worker(worker):
        tenant = await create_isolated_tenant(cp, "cost-switch")
        wf = await cp.create_workflow("cost_switch", tenant.id, config=WorkflowConfig(version=1))
        try:
            await cp.set_agent_lifecycle_policy(wf.id, "analyst", [
                AgentRule(metric=AgentMetric.COST_USD, op=Op.GTE, threshold=1.0,
                          window=1, action=SetModel(value=HAIKU)),
            ])
            await cp.activate_workflow(wf.id)

            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, wf.id)

            await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
            await wait_for_completion(cp, wf.id)
        finally:
            await cp.delete_workflow(wf.id)

    assert len(models) == 2, f"expected 2 runs, got {models}"
    assert models[0] == OPUS, f"first run should be Opus, was {models[0]}"
    assert models[1] == HAIKU, f"cost policy should switch run 2 to Haiku, was {models[1]}"


# ── Cost-function units (the cache path the mock can't produce) ───────────────

from boundflow.llm import Usage, _estimate_cost  # noqa: E402


def test_cost_is_cache_aware():
    """Cache writes bill at 1.25x and reads at 0.1x the input rate — a path the
    mock LLM can't exercise (it never returns cache tokens)."""
    pricing = {"claude-opus-4-8": {"input_per_1m": 5.0, "output_per_1m": 25.0}}
    u = Usage(input_tokens=300, output_tokens=150,
              cache_creation_input_tokens=200_000, cache_read_input_tokens=1_000_000)
    # input units = 300 + 200_000*1.25 + 1_000_000*0.1 = 350_300
    expected = 350_300 / 1e6 * 5.0 + 150 / 1e6 * 25.0
    assert round(_estimate_cost(u, "claude-opus-4-8", pricing), 9) == round(expected, 9)


def test_cost_prefix_matches_dated_model_id():
    """A dated model id resolves to its alias pricing entry by prefix."""
    pricing = {"claude-haiku-4-5": {"input_per_1m": 1.0, "output_per_1m": 5.0}}
    u = Usage(input_tokens=1_000_000, output_tokens=0)
    assert _estimate_cost(u, "claude-haiku-4-5-20251001", pricing) == 1.0


def test_cost_unknown_model_is_zero():
    """An unpriced model records 0 cost rather than crashing."""
    u = Usage(input_tokens=1_000_000, output_tokens=1_000_000)
    assert _estimate_cost(u, "some-unpriced-model", {}) == 0.0
