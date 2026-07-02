"""boundflow pricing — model pricing CLI tests."""
from __future__ import annotations

import pytest

from .conftest import run

DEFAULTS = {
    "claude-opus-4-8": (5.0, 25.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-haiku-4-5": (1.0, 5.0),
}


async def _reset(runner, api_key):
    """Restore seeded defaults after a mutating test."""
    for model_id, (in_rate, out_rate) in DEFAULTS.items():
        run(runner, api_key, [
            "pricing", "set", model_id,
            "--input", str(in_rate),
            "--output", str(out_rate),
        ])


def test_list_pricing_returns_list(runner, boundflow_api_key):
    results = run(runner, boundflow_api_key, ["pricing", "list"])
    assert isinstance(results, list)
    assert len(results) > 0


def test_list_pricing_shows_seeded_defaults(runner, boundflow_api_key):
    results = run(runner, boundflow_api_key, ["pricing", "list"])
    by_model = {r["model"]: r for r in results}

    assert "claude-opus-4-8" in by_model
    assert "claude-sonnet-4-6" in by_model
    assert "claude-haiku-4-5" in by_model

    opus = by_model["claude-opus-4-8"]
    assert opus["input_per_1m_usd"] == 5.0
    assert opus["output_per_1m_usd"] == 25.0

    sonnet = by_model["claude-sonnet-4-6"]
    assert sonnet["input_per_1m_usd"] == 3.0
    assert sonnet["output_per_1m_usd"] == 15.0

    haiku = by_model["claude-haiku-4-5"]
    assert haiku["input_per_1m_usd"] == 1.0
    assert haiku["output_per_1m_usd"] == 5.0


def test_set_pricing_updates_model(runner, boundflow_api_key):
    try:
        result = run(runner, boundflow_api_key, [
            "pricing", "set", "claude-opus-4-8",
            "--input", "7.0", "--output", "35.0",
        ])
        assert result["status"] == "ok"

        listing = run(runner, boundflow_api_key, ["pricing", "list"])
        by_model = {r["model"]: r for r in listing}
        assert by_model["claude-opus-4-8"]["input_per_1m_usd"] == 7.0
        assert by_model["claude-opus-4-8"]["output_per_1m_usd"] == 35.0
    finally:
        run(runner, boundflow_api_key, [
            "pricing", "set", "claude-opus-4-8", "--input", "5.0", "--output", "25.0",
        ])


def test_set_pricing_only_affects_target_model(runner, boundflow_api_key):
    """Overriding one model does not change the others."""
    try:
        run(runner, boundflow_api_key, [
            "pricing", "set", "claude-sonnet-4-6",
            "--input", "9.9", "--output", "49.9",
        ])
        listing = run(runner, boundflow_api_key, ["pricing", "list"])
        by_model = {r["model"]: r for r in listing}

        # Sonnet changed
        assert by_model["claude-sonnet-4-6"]["input_per_1m_usd"] == 9.9
        # Haiku untouched
        assert by_model["claude-haiku-4-5"]["input_per_1m_usd"] == 1.0
    finally:
        run(runner, boundflow_api_key, [
            "pricing", "set", "claude-sonnet-4-6", "--input", "3.0", "--output", "15.0",
        ])


def test_list_pricing_json_has_expected_keys(runner, boundflow_api_key):
    results = run(runner, boundflow_api_key, ["pricing", "list"])
    for row in results:
        assert "model" in row
        assert "input_per_1m_usd" in row
        assert "output_per_1m_usd" in row
