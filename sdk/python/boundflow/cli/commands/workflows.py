"""boundflow workflow — workflow lifecycle commands."""

import json
from typing import Optional

import typer

from boundflow.cli._client import cp_call
from boundflow.cli._output import error, output, success
from boundflow.control_plane import InvokeMode, WorkflowConfig

app = typer.Typer(help="Manage workflows.")


@app.command("create")
def create(
    workflow_type: str = typer.Argument(..., help="Workflow type identifier"),
    tenant_id: str = typer.Argument(..., help="Tenant ID to own this workflow"),
    version: int = typer.Option(1, "--version", "-v", help="Workflow version"),
    timeout: int = typer.Option(60, "--timeout", help="Invoke timeout in seconds"),
    repeat: int = typer.Option(0, "--repeat", help="Repeat every N seconds (0 = no repeat)"),
    no_triggerable: bool = typer.Option(False, "--no-triggerable", help="Disable manual triggering"),
    invoke_mode: InvokeMode = typer.Option(
        InvokeMode.COALESCE, "--invoke-mode",
        help="How piled-up invokes are handled: coalesce (latest-wins) or queue (run each, FIFO)"),
    max_queue_depth: int = typer.Option(
        0, "--max-queue-depth",
        help="Queue-mode backlog cap; invokes past it are rejected (0 = server default)"),
):
    """Create a new workflow."""
    config = WorkflowConfig(
        version=version,
        invoke_timeout_seconds=timeout,
        repeat_every_seconds=repeat,
        triggerable=not no_triggerable,
        invoke_mode=invoke_mode,
        max_queue_depth=max_queue_depth,
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
    """Get a workflow's current version, lifecycle state, and runtime state."""
    result = cp_call(lambda cp: cp.get_workflow(workflow_id))
    output(result)


@app.command("metrics")
def metrics(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
):
    """Cumulative totals (cost, run count, failures, ...) for a workflow's current version."""
    result = cp_call(lambda cp: cp.get_workflow_metrics(workflow_id))
    output(result)


@app.command("list")
def list_workflows():
    """List all workflows in the tenant group."""
    results = cp_call(lambda cp: cp.list_workflows())
    output(results)


@app.command("runs")
def runs(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
):
    """List a workflow's runs, most recent first, with each run's status and outcome."""
    results = cp_call(lambda cp: cp.list_workflow_runs(workflow_id))
    output(results)


@app.command("request")
def request(
    request_id: str = typer.Argument(..., help="Request ID (returned by 'workflow invoke')"),
):
    """Show the status and outcome of a single run, by its request ID."""
    result = cp_call(lambda cp: cp.get_request_info(request_id))
    output(result)


@app.command("resolve")
def resolve(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    request_id: str = typer.Argument(..., help="The interrupted run's request ID (last_interrupted_request_id)"),
):
    """Clear a platform interruption and re-activate an interrupted workflow."""
    cp_call(lambda cp: cp.resolve_interrupted_workflow(workflow_id, request_id))
    success(f"Workflow {workflow_id} resolved and re-activated.")


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


@app.command("submit-input")
def submit_input(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    input_id: str = typer.Argument(..., help="Input ID (from the input request)"),
    answer: str = typer.Option(..., "--answer", help="Answer as a JSON object, e.g. '{\"choice\": \"refund\"}'"),
    actor: str = typer.Option("", "--actor", help="Who supplied the answer (e.g. email or user ID)"),
):
    """Answer a pending workflow input gate."""
    try:
        parsed = json.loads(answer)
    except json.JSONDecodeError as e:
        error(f"--answer must be valid JSON: {e}")
        raise typer.Exit(1)
    cp_call(lambda cp: cp.submit_input(workflow_id, input_id, parsed, actor=actor))
    success(f"Input {input_id} submitted.")


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
