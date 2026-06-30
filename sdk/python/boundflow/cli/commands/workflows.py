"""bfc workflow — workflow lifecycle commands."""

from typing import Optional

import typer

from boundflow.cli._client import cp_call
from boundflow.cli._output import output, success
from boundflow.control_plane import WorkflowConfig

app = typer.Typer(help="Manage workflows.")


@app.command("create")
def create(
    workflow_type: str = typer.Argument(..., help="Workflow type identifier"),
    tenant_id: str = typer.Argument(..., help="Tenant ID to own this workflow"),
    version: int = typer.Option(1, "--version", "-v", help="Workflow version"),
    timeout: int = typer.Option(60, "--timeout", help="Invoke timeout in seconds"),
    repeat: int = typer.Option(0, "--repeat", help="Repeat every N seconds (0 = no repeat)"),
    no_triggerable: bool = typer.Option(False, "--no-triggerable", help="Disable manual triggering"),
):
    """Create a new workflow."""
    config = WorkflowConfig(
        version=version,
        invoke_timeout_seconds=timeout,
        repeat_every_seconds=repeat,
        triggerable=not no_triggerable,
    )
    result = cp_call(lambda cp: cp.create_workflow(workflow_type, tenant_id, config=config))
    output(result)


@app.command("activate")
def activate(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
):
    """Transition a workflow from 'creating' to 'active'."""
    cp_call(lambda cp: cp.activate_workflow(workflow_id))
    success(f"Workflow {workflow_id} activated.")


@app.command("invoke")
def invoke(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    op_timeout: int = typer.Option(0, "--op-timeout", help="Per-operation timeout in seconds (0 = server default)"),
):
    """Trigger a workflow run. Prints the request_id for tracing."""
    request_id = cp_call(lambda cp: cp.invoke_workflow(workflow_id, operation_timeout_seconds=op_timeout))
    output({"request_id": request_id})


@app.command("get")
def get(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
):
    """Get a workflow's current lifecycle and runtime state."""
    async def _get(cp):
        lc = await cp.get_workflow_lifecycle_state(workflow_id)
        wf = await cp.get_workflow_state(workflow_id)
        return {"workflow_id": workflow_id, "lifecycle_state": lc.value, "workflow_state": wf.value if wf else None}

    result = cp_call(_get)
    output(result)


@app.command("list")
def list_workflows():
    """List all workflows in the tenant group."""
    results = cp_call(lambda cp: cp.list_workflows())
    output(results)


@app.command("approve")
def approve(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    approval_id: str = typer.Argument(..., help="Approval ID (from the approval request)"),
    actor: str = typer.Option("", "--actor", help="Approver identity (e.g. email or user ID)"),
):
    """Approve a pending workflow approval gate."""
    cp_call(lambda cp: cp.approve_workflow(workflow_id, approval_id, actor=actor))
    success(f"Approval {approval_id} approved.")


@app.command("reject")
def reject(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    approval_id: str = typer.Argument(..., help="Approval ID (from the approval request)"),
    actor: str = typer.Option("", "--actor", help="Rejecter identity (e.g. email or user ID)"),
):
    """Reject a pending workflow approval gate."""
    cp_call(lambda cp: cp.reject_workflow(workflow_id, approval_id, actor=actor))
    success(f"Approval {approval_id} rejected.")


@app.command("delete")
def delete(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Delete a workflow."""
    if not yes:
        typer.confirm(f"Delete workflow {workflow_id}?", abort=True)
    cp_call(lambda cp: cp.delete_workflow(workflow_id))
    success(f"Workflow {workflow_id} deleted.")
