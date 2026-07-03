"""Rich table rendering and --json output helpers."""

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum

import typer
from rich.console import Console
from rich.table import Table

console = Console()
_json_mode: bool = False


def set_json(value: bool) -> None:
    global _json_mode
    _json_mode = value


def _normalize(value):
    """Recursively convert enum instances to their .value, so table display
    and JSON output both show plain strings rather than 'EnumClass.member'."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    return value


def _to_dict(obj) -> dict:
    if is_dataclass(obj):
        raw = asdict(obj)
    elif isinstance(obj, dict):
        raw = obj
    else:
        raw = vars(obj)
    return _normalize(raw)


def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


def output(data) -> None:
    """Render a single record (dict/dataclass) or a list of them."""
    if isinstance(data, list):
        rows = [_to_dict(r) for r in data]
        if _json_mode:
            typer.echo(json.dumps(rows, default=_json_default, indent=2))
        else:
            _table(rows)
    else:
        rec = _to_dict(data)
        if _json_mode:
            typer.echo(json.dumps(rec, default=_json_default, indent=2))
        else:
            _record(rec)


def _table(rows: list[dict]) -> None:
    if not rows:
        console.print("[dim]No results.[/dim]")
        return
    t = Table(show_header=True, header_style="bold cyan")
    for key in rows[0]:
        t.add_column(str(key))
    for row in rows:
        t.add_row(*[str(v) if v is not None else "" for v in row.values()])
    console.print(t)


def _record(rec: dict) -> None:
    t = Table(show_header=False, box=None, padding=(0, 1))
    t.add_column("Key", style="bold cyan", no_wrap=True)
    t.add_column("Value")
    for k, v in rec.items():
        t.add_row(str(k), str(v) if v is not None else "")
    console.print(t)


def success(msg: str) -> None:
    if _json_mode:
        typer.echo(json.dumps({"status": "ok", "message": msg}))
    else:
        console.print(f"[green]{msg}[/green]")
