"""Async bridge between sync Typer commands and the async ControlPlaneClient."""

import asyncio
import os

import typer

from boundflow.cli._output import error as report_error
from boundflow.control_plane import ControlPlaneClient, DEFAULT_SERVER_ADDRESS

_server: str = DEFAULT_SERVER_ADDRESS
_api_key: str = ""


def configure(server: str, api_key: str) -> None:
    # Runs in the root callback for every invocation — including `--help` — so it
    # only resolves config, never fails. The API key is required lazily in cp_call,
    # when a command actually calls the control plane.
    global _server, _api_key
    _server = server or os.environ.get("BOUNDFLOW_SERVER_ADDRESS") or DEFAULT_SERVER_ADDRESS
    _api_key = api_key or os.environ.get("BOUNDFLOW_API_KEY") or ""


def cp_call(fn):
    """Open a ControlPlaneClient, call fn(client), close, return result.

    Converts any exception into a user-facing error message + Exit(1).
    """
    if not _api_key:
        report_error("no API key. Set BOUNDFLOW_API_KEY or pass --api-key.")
        raise typer.Exit(1)

    async def _run():
        async with ControlPlaneClient(_server, api_key=_api_key) as cp:
            return await fn(cp)

    try:
        return asyncio.run(_run())
    except typer.Exit:
        raise
    except Exception as exc:
        report_error(str(exc))
        raise typer.Exit(1)
