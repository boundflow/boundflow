"""Metering from the harness's own state.

The rule under test: where both sides can produce a number, the harness's wins — and it
is counted exactly once, however many times the harness writes it.
"""
from __future__ import annotations

import asyncio

import pytest

from boundflow.governed import AgentGovernor
from boundflow.harness import UngovernedModel, validate_subagents
from boundflow.harness_metering import metered
from boundflow.policies import RuntimePolicy

PRICING = {"claude-sonnet-4-6": {"input_per_1m": 3.0, "output_per_1m": 15.0}}


class _Message:
    """An AIMessage, as far as metering is concerned."""

    def __init__(self, id, input_tokens=0, output_tokens=0, cache_read=0,
                 model="claude-sonnet-4-6", tool_calls=()):
        self.id = id
        self.usage_metadata = {
            "input_tokens": input_tokens, "output_tokens": output_tokens,
            "input_token_details": {"cache_creation": 0, "cache_read": cache_read}}
        self.response_metadata = {"model_name": model}
        self.tool_calls = [{"name": n} for n in tool_calls]


class _Saver:
    """Records what reached the real checkpointer."""

    def __init__(self):
        self.puts, self.writes = [], []
        self.serde = "passthrough-attribute"

    async def aput(self, config, checkpoint, metadata, new_versions):
        self.puts.append(checkpoint)
        return {"ok": True}

    async def aput_writes(self, config, writes, task_id, task_path=""):
        self.writes.append(writes)


def _governor() -> AgentGovernor:
    return AgentGovernor("operator", RuntimePolicy(), "claude-sonnet-4-6", PRICING,
                         collect_spans=False)


def _checkpoint(*messages):
    return {"channel_values": {"messages": list(messages)}}


def test_usage_is_taken_from_the_message():
    governor = _governor()
    saver = metered(_Saver(), governor)
    asyncio.run(saver.aput({}, _checkpoint(_Message("a", 1000, 500)), {}, {}))

    assert governor.observed_llm_calls == 1
    assert governor.tokens_used == 1500
    # 1000 in at $3/M + 500 out at $15/M
    assert governor.cost_usd == pytest.approx(1000 / 1e6 * 3 + 500 / 1e6 * 15)


def test_a_message_is_counted_once_however_often_it_is_written():
    # The failure this guards against is silent and large: a message appears in the
    # writes and again in every later checkpoint, so counting per write multiplies a
    # run's spend by the number of super-steps.
    governor = _governor()
    saver = metered(_Saver(), governor)
    first, second = _Message("a", 1000, 500), _Message("b", 2000, 100)

    asyncio.run(saver.aput_writes({}, [("messages", first)], "task-1"))
    asyncio.run(saver.aput({}, _checkpoint(first), {}, {}))
    asyncio.run(saver.aput({}, _checkpoint(first, second), {}, {}))
    asyncio.run(saver.aput({}, _checkpoint(first, second), {}, {}))

    assert governor.observed_llm_calls == 2
    assert governor.tokens_used == 1500 + 2100


def test_harness_numbers_supersede_ours_rather_than_adding():
    governor = _governor()
    metered(_Saver(), governor)
    call = governor.begin_call()
    from boundflow.llm import Usage
    call.record(Usage(input_tokens=1000, output_tokens=500))

    # Our own path stopped accumulating: the harness will report this same call when it
    # writes the message, and double counting it is the whole hazard.
    assert governor.cost_usd == 0
    assert governor.tokens_used == 0
    # The reservation still happened — enforcement doesn't depend on who reports.
    assert governor.llm_calls == 1


def test_snapshot_reports_the_harness_count():
    governor = _governor()
    saver = metered(_Saver(), governor)
    # Two calls the governor never saw — a subagent that built its own client.
    asyncio.run(saver.aput({}, _checkpoint(_Message("a", 10, 5), _Message("b", 10, 5)), {}, {}))
    assert governor.snapshot()["llm_calls"] == 2


def test_cache_reads_are_priced_as_reads():
    governor = _governor()
    saver = metered(_Saver(), governor)
    asyncio.run(saver.aput({}, _checkpoint(_Message("a", 100, 0, cache_read=900)), {}, {}))
    assert governor.tokens_used == 1000
    # Cheaper than 1000 fresh input tokens, or the cache split isn't being read.
    assert governor.cost_usd < 1000 / 1e6 * 3


def test_state_still_reaches_the_real_saver():
    governor = _governor()
    inner = _Saver()
    saver = metered(inner, governor)
    result = asyncio.run(saver.aput({}, _checkpoint(_Message("a", 1, 1)), {}, {}))
    assert result == {"ok": True} and len(inner.puts) == 1


def test_a_metering_failure_never_costs_the_customer_their_state():
    class _Exploding:
        id = "boom"
        @property
        def usage_metadata(self):
            raise RuntimeError("bad message")

    governor = _governor()
    inner = _Saver()
    saver = metered(inner, governor)
    asyncio.run(saver.aput({}, _checkpoint(_Exploding()), {}, {}))
    assert len(inner.puts) == 1  # the write went through regardless


def test_unrecognised_channels_are_ignored():
    governor = _governor()
    saver = metered(_Saver(), governor)
    asyncio.run(saver.aput_writes({}, [("todos", {"x": 1}), ("files", "notes.md")], "t"))
    assert governor.observed_llm_calls == 0


def test_passthrough_for_everything_not_metered():
    saver = metered(_Saver(), _governor())
    assert saver.serde == "passthrough-attribute"


def test_string_models_are_rejected():
    with pytest.raises(UngovernedModel, match="uncapped"):
        validate_subagents([{"name": "scribe", "model": "claude-sonnet-4-6"}])
    # Inheriting the parent's governed model is the supported shape.
    assert validate_subagents([{"name": "scribe"}]) == [{"name": "scribe"}]


def test_messages_nested_in_a_write_are_found():
    # The real write shape: [(channel, value)] where value is a *list* of messages.
    # Handling only one level of nesting skips every parent-agent call — its own
    # checkpoints are deltas with empty channel_values, so the write is the only
    # place it is ever seen.
    governor = _governor()
    saver = metered(_Saver(), governor)
    asyncio.run(saver.aput_writes(
        {}, [("messages", [_Message("a", 1000, 500)]), ("branch:to:model", None)], "t"))
    assert governor.observed_llm_calls == 1
    assert governor.tokens_used == 1500
