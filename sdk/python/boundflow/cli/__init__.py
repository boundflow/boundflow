"""bfc — BoundFlow control plane CLI."""

import typer

from boundflow.cli._client import configure
from boundflow.cli._output import set_json
from boundflow.cli.commands import audit, policies, pricing, tenants, workflows

app = typer.Typer(
    name="bfc",
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


def main() -> None:
    app()
