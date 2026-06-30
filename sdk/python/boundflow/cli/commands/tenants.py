"""bfc tenant — tenant group and tenant management commands."""

import typer

from boundflow.cli._client import cp_call
from boundflow.cli._output import output, success

app = typer.Typer(help="Manage tenant groups and tenants.")

group_app = typer.Typer(help="Manage tenant groups.")
app.add_typer(group_app, name="group")


@group_app.command("get")
def group_get(tenant_group_id: str = typer.Argument(..., help="Tenant group ID")):
    """Get a tenant group by ID."""
    result = cp_call(lambda cp: cp.get_tenant_group(tenant_group_id))
    output(result)


@group_app.command("delete")
def group_delete(
    tenant_group_id: str = typer.Argument(..., help="Tenant group ID"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Delete a tenant group (and all its tenants and workflows)."""
    if not yes:
        typer.confirm(f"Delete tenant group {tenant_group_id}? This is irreversible.", abort=True)
    cp_call(lambda cp: cp.delete_tenant_group(tenant_group_id))
    success(f"Tenant group {tenant_group_id} deleted.")


@app.command("create")
def tenant_create(name: str = typer.Argument(..., help="Tenant name")):
    """Create a new tenant (inside the API key's tenant group)."""
    result = cp_call(lambda cp: cp.create_tenant(name))
    output(result)


@app.command("get")
def tenant_get(tenant_id: str = typer.Argument(..., help="Tenant ID")):
    """Get a tenant by ID."""
    result = cp_call(lambda cp: cp.get_tenant(tenant_id))
    output(result)


@app.command("delete")
def tenant_delete(
    tenant_id: str = typer.Argument(..., help="Tenant ID"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Delete a tenant and all its workflows."""
    if not yes:
        typer.confirm(f"Delete tenant {tenant_id}?", abort=True)
    cp_call(lambda cp: cp.delete_tenant(tenant_id))
    success(f"Tenant {tenant_id} deleted.")
