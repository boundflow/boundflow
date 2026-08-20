"""Reporting spend before the operation ends.

The point is what survives a crash: an authorised call is on record at its worst case
before the money is spent, and replaced by the real cost once that is known.
"""
from __future__ import annotations

import asyncio

import pytest

from boundflow.governed import AgentGovernor
from boundflow.llm import Usage
from boundflow.policies import RuntimePolicy

PRICING = {"claude-sonnet-4-6": {"input_per_1m": 3.0, "output_per_1m": 15.0}}


def _governor(**policy) -> AgentGovernor:
    return AgentGovernor("operator", RuntimePolicy(**policy), "claude-sonnet-4-6",
                         PRICING, collect_spans=False)


def test_an_authorised_call_is_on_record_before_it_is_made():
    governor = _governor(max_tokens_per_call=1000)
    assert governor.snapshot()["cost_usd"] == 0

    governor.begin_call()
    # Nothing has been recorded, but the worst case is already reportable: output
    # capped at max_tokens, input estimated from the pad.
    reserved = governor.snapshot()["cost_usd"]
    assert reserved == pytest.approx(2000 / 1e6 * 3 + 1000 / 1e6 * 15)


def test_the_real_cost_replaces_the_reservation():
    governor = _governor(max_tokens_per_call=1000)
    call = governor.begin_call()
    call.record(Usage(input_tokens=500, output_tokens=100))

    # Only what the call actually cost — the reservation is released, not added.
    assert governor.snapshot()["cost_usd"] == pytest.approx(500 / 1e6 * 3 + 100 / 1e6 * 15)
    assert governor.reserved_cost_usd == 0


def test_an_abandoned_call_releases_its_reservation():
    # The provider refused, so nothing was billed and nothing should be held.
    governor = _governor(max_tokens_per_call=1000)
    call = governor.begin_call()
    call.abandon()
    assert governor.reserved_cost_usd == 0
    assert governor.snapshot()["cost_usd"] == 0


def test_the_estimate_tightens_as_the_conversation_grows():
    # Input is estimated from the last call's exact count, so only the messages added
    # since are guessed at — the error doesn't compound across a run.
    governor = _governor(max_tokens_per_call=1000)
    call = governor.begin_call()
    call.record(Usage(input_tokens=8000, output_tokens=100))
    governor.begin_call()
    assert governor.reserved_cost_usd == pytest.approx(
        10_000 / 1e6 * 3 + 1000 / 1e6 * 15)


def test_reservations_stack_for_concurrent_calls():
    # Parallel subagents share one governor; each holds its own worst case until it
    # is answered for.
    governor = _governor(max_tokens_per_call=1000)
    first, second = governor.begin_call(), governor.begin_call()
    both = governor.reserved_cost_usd
    first.record(Usage(input_tokens=100, output_tokens=10))
    assert governor.reserved_cost_usd == pytest.approx(both / 2)
    second.record(Usage(input_tokens=100, output_tokens=10))
    assert governor.reserved_cost_usd == 0


def test_harness_actuals_release_reservations_too():
    # Under harness metering our own record() stops accumulating, but the reservation
    # still has to be released when the harness reports the real number.
    governor = _governor(max_tokens_per_call=1000)
    governor.register_harness_metering()
    governor.begin_call()
    assert governor.reserved_cost_usd > 0
    governor.record_harness_usage(input_tokens=500, output_tokens=100, details={},
                                  model="claude-sonnet-4-6")
    assert governor.reserved_cost_usd == 0
    assert governor.snapshot()["cost_usd"] == pytest.approx(
        500 / 1e6 * 3 + 100 / 1e6 * 15)


def test_reports_bracket_the_model_call():
    """The ordering that makes the reservation worth anything: it leaves before the
    call does."""
    from boundflow.langchain_client import GovernedChatModel
    from langchain_core.messages import AIMessage

    events: list[tuple[str, float]] = []
    governor = _governor(max_tokens_per_call=1000)

    async def report():
        events.append(("report", governor.snapshot()["cost_usd"]))

    class _Model:
        async def ainvoke(self, messages, **kwargs):
            events.append(("call", governor.snapshot()["cost_usd"]))
            return AIMessage(content="ok", usage_metadata={
                "input_tokens": 500, "output_tokens": 100, "total_tokens": 600})

        def bind(self, **kwargs):
            return self

    model = GovernedChatModel(governor=governor, chat_model=_Model(), report=report)
    asyncio.run(model._agenerate([]))

    assert [name for name, _ in events] == ["report", "call", "report"]
    # The pre-call report already carries the worst case; the post-call one carries
    # the real, smaller number.
    assert events[0][1] > 0 and events[2][1] < events[0][1]
