"""boundflow audit — approval and policy audit log commands."""

from dataclasses import asdict
from typing import Optional

import typer

from boundflow.cli._client import cp_call
from boundflow.cli._output import output

app = typer.Typer(help="View approval and policy audit records.")


def _flatten(record) -> dict:
    """Flatten a dataclass to a dict, serialising nested structures to strings."""
    d = asdict(record) if hasattr(record, "__dataclass_fields__") else vars(record)
    return {k: str(v) if isinstance(v, (list, dict)) else v for k, v in d.items()}


@app.command("approvals")
def approvals(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    approval_id: Optional[str] = typer.Option(None, "--approval-id", help="Look up a single approval by ID"),
):
    """List approval decisions for a workflow (or fetch one by approval ID)."""
    if approval_id:
        result = cp_call(lambda cp: cp.get_approval_audit_by_id(approval_id))
        if result is None:
            typer.echo(f"No approval found with ID {approval_id}.", err=True)
            raise typer.Exit(1)
        output(_flatten(result))
    else:
        results = cp_call(lambda cp: cp.get_approval_audit(workflow_id))
        output([_flatten(r) for r in results])


@app.command("workflow")
def workflow_policy(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
):
    """List workflow-lifecycle policy firings for a workflow."""
    results = cp_call(lambda cp: cp.get_workflow_policy_audit(workflow_id))
    output([_flatten(r) for r in results])


@app.command("agent")
def agent_policy(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    agent_name: str = typer.Argument(..., help="Agent name"),
):
    """List agent-lifecycle policy firings for a specific agent."""
    results = cp_call(lambda cp: cp.get_agent_policy_audit(workflow_id, agent_name))
    output([_flatten(r) for r in results])


@app.command("log")
def log(
    workflow_id: Optional[str] = typer.Argument(None, help="Workflow ID (omit for tenant-wide log)"),
):
    """Show the unified audit log (approvals + policy firings, newest first)."""
    results = cp_call(lambda cp: cp.get_audit_log(workflow_id or ""))
    output([_flatten(r) for r in results])
