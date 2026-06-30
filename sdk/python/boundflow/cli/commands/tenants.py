"""bfc tenant — tenant group and tenant management commands."""

import typer

from boundflow.cli._client import cp_call
from boundflow.cli._output import output, success

app = typer.Typer(help="Manage tenant groups and tenants.")

group_app = typer.Typer(help="Manage tenant groups.")
app.add_typer(group_app, name="group")


@group_app.command("create")
def group_create(name: str = typer.Argument(..., help="Tenant group name")):
    """Create a new tenant group."""
    result = cp_call(lambda cp: cp.create_tenant_group(name))
    output(result)


@app.command("create")
def tenant_create(name: str = typer.Argument(..., help="Tenant name")):
    """Create a new tenant (inside the API key's tenant group)."""
    result = cp_call(lambda cp: cp.create_tenant(name))
    output(result)
