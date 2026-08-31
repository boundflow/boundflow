"""boundflow — BoundFlow control plane CLI."""

import typer

from boundflow.cli._client import configure, resolved
from boundflow.cli.output import error, set_json
from boundflow.cli.commands import audit, policies, pricing, tenants, workflows

app = typer.Typer(
    name="boundflow",
    help="BoundFlow control plane CLI — manage workflows, policies, and audit logs.",
    no_args_is_help=True,
)

app.add_typer(tenants.app, name="tenant")
app.add_typer(workflows.app, name="workflow")
app.add_typer(policies.app, name="policy")
app.add_typer(audit.app, name="audit")
app.add_typer(pricing.app, name="pricing")


@app.callback()
def root(
    server: str = typer.Option(
        "", "--server", envvar="BOUNDFLOW_SERVER_ADDRESS",
        help="gRPC server address (default: http://localhost:50051)",
    ),
    api_key: str = typer.Option(
        "", "--api-key", envvar="BOUNDFLOW_API_KEY",
        help="BoundFlow API key",
    ),
    json_output: bool = typer.Option(
        False, "--json",
        help="Output raw JSON (useful for scripting)",
    ),
):
    set_json(json_output)
    configure(server, api_key)


@app.command("ui")
def ui(
    port: int = typer.Option(8787, "--port", help="Port to serve on (localhost only)"),
    no_browser: bool = typer.Option(False, "--no-browser", help="Don't open a browser"),
):
    """Serve the local operator console: the fleet, its gates, and holds."""
    from boundflow.ui import serve

    server, api_key = resolved()
    if not api_key:
        error("no API key. Set BOUNDFLOW_API_KEY or pass --api-key.")
        raise typer.Exit(1)
    serve(server, api_key, port=port, open_browser=not no_browser)


def main() -> None:
    app()
