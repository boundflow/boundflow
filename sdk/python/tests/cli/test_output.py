from __future__ import annotations

import pytest

from boundflow.cli import output as output_mod


class _TableSpy:
    def __init__(self, **_kwargs):
        self.columns = []
        self.rows = []

    def add_column(self, name: str) -> None:
        self.columns.append(name)

    def add_row(self, *values: str) -> None:
        self.rows.append(values)


@pytest.mark.parametrize(
    ("rows", "expected_columns", "expected_rows"),
    [
        (
            [
                {"approval_id": "approval-1", "actor": "alice"},
                {
                    "metric": "approval_rejections",
                    "action": "set_version",
                    "actor": "policy",
                },
            ],
            ["approval_id", "actor", "metric", "action"],
            [
                ("approval-1", "alice", "", ""),
                ("", "policy", "approval_rejections", "set_version"),
            ],
        ),
        (
            [
                {"metric": "approval_rejections", "action": "set_version"},
                {"approval_id": "approval-1", "actor": "alice"},
            ],
            ["metric", "action", "approval_id", "actor"],
            [
                ("approval_rejections", "set_version", "", ""),
                ("", "", "approval-1", "alice"),
            ],
        ),
    ],
)
def test_table_aligns_heterogeneous_rows_by_key(
    monkeypatch, rows, expected_columns, expected_rows
):
    rendered = []
    monkeypatch.setattr(output_mod, "Table", _TableSpy)
    monkeypatch.setattr(output_mod.console, "print", rendered.append)

    output_mod._table(rows)

    assert rendered[0].columns == expected_columns
    assert rendered[0].rows == expected_rows


def test_table_preserves_homogeneous_row_layout(monkeypatch):
    rendered = []
    monkeypatch.setattr(output_mod, "Table", _TableSpy)
    monkeypatch.setattr(output_mod.console, "print", rendered.append)

    output_mod._table([{"name": "one", "count": 0}, {"name": "two", "count": None}])

    assert rendered[0].columns == ["name", "count"]
    assert rendered[0].rows == [("one", "0"), ("two", "")]
