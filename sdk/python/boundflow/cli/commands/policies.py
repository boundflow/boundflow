"""boundflow policy — runtime and lifecycle policy management."""

import json
from pathlib import Path
from typing import List, Optional

import typer

from boundflow.cli._client import cp_call
from boundflow.cli._output import output, success
from boundflow.policies import AgentRule, RuntimePolicy, ToolCallLimit, WorkflowRule

app = typer.Typer(help="Manage agent and workflow policies.")
lifecycle_app = typer.Typer(help="Manage lifecycle (auto-remediation) policies.")
app.add_typer(lifecycle_app, name="lifecycle")


def _parse_tool_limit(value: str) -> ToolCallLimit:
    """Parse 'tool:max' string into ToolCallLimit."""
    parts = value.split(":", 1)
    if len(parts) != 2:
        raise typer.BadParameter(f"Expected TOOL:MAX format, got: {value!r}")
    try:
        return ToolCallLimit(tool=parts[0], max_calls=int(parts[1]))
    except ValueError:
        raise typer.BadParameter(f"Max calls must be an integer, got: {parts[1]!r}")


def _load_json(file: Optional[Path], inline: Optional[str]) -> list:
    if file and inline:
        typer.echo("Error: provide --file or --rules, not both.", err=True)
        raise typer.Exit(1)
    if file:
        return json.loads(file.read_text(encoding="utf-8-sig"))
    if inline:
        return json.loads(inline)
    typer.echo("Error: provide --file or --rules.", err=True)
    raise typer.Exit(1)


@app.command("runtime")
def runtime_set(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    agent_name: str = typer.Argument(..., help="Agent name"),
    model: Optional[str] = typer.Option(None, "--model", help="Override the agent's model"),
    max_llm_calls: Optional[int] = typer.Option(None, "--max-llm-calls", help="Max LLM calls per run"),
    max_cost_usd: Optional[float] = typer.Option(None, "--max-cost-usd", help="Max cost in USD per run"),
    max_tokens_per_call: Optional[int] = typer.Option(None, "--max-tokens-per-call", help="Max tokens per LLM call"),
    tool_limit: List[str] = typer.Option([], "--tool-limit", help="Per-tool call limit as TOOL:MAX (repeatable)"),
    file: Optional[Path] = typer.Option(None, "--file", help="JSON file with a RuntimePolicy object (overrides all flags)"),
):
    """Set the runtime (hard cap) policy for an agent."""
    if file:
        policy = RuntimePolicy.model_validate(json.loads(file.read_text(encoding="utf-8-sig")))
    else:
        limits = [_parse_tool_limit(t) for t in tool_limit]
        policy = RuntimePolicy(
            model=model,
            max_llm_calls=max_llm_calls or 0,
            max_cost_usd=max_cost_usd or 0.0,
            max_tokens_per_call=max_tokens_per_call or 0,
            tool_call_limits=limits,
        )
    cp_call(lambda cp: cp.set_agent_runtime_policy(workflow_id, agent_name, policy))
    success(f"Runtime policy set on {workflow_id} / {agent_name}.")


@app.command("get-runtime")
def runtime_get(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    agent_name: str = typer.Argument(..., help="Agent name"),
):
    """Show the armed runtime (hard cap) policy for an agent (empty if none is set)."""
    policy = cp_call(lambda cp: cp.get_agent_runtime_policy(workflow_id, agent_name))
    output(policy or {})


@lifecycle_app.command("set-agent")
def lifecycle_set_agent(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    agent_name: str = typer.Argument(..., help="Agent name"),
    file: Optional[Path] = typer.Option(None, "--file", help="JSON file with a list of AgentRule objects"),
    rules: Optional[str] = typer.Option(None, "--rules", help="Inline JSON list of AgentRule objects"),
):
    """Set the lifecycle (auto-remediation) rules for an agent.

    Rules are provided as JSON. Example rule:
    [{"metric":"cost_usd","op":"greater_than","threshold":0.20,"window":5,"action":{"field":"model","value":"claude-haiku-4-5"}}]
    """
    raw = _load_json(file, rules)
    agent_rules = [AgentRule.model_validate(r) for r in raw]
    cp_call(lambda cp: cp.set_agent_lifecycle_policy(workflow_id, agent_name, agent_rules))
    success(f"Agent lifecycle policy set on {workflow_id} / {agent_name}.")


@lifecycle_app.command("set-workflow")
def lifecycle_set_workflow(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    file: Optional[Path] = typer.Option(None, "--file", help="JSON file with a list of WorkflowRule objects"),
    rules: Optional[str] = typer.Option(None, "--rules", help="Inline JSON list of WorkflowRule objects"),
):
    """Set the lifecycle rules for a workflow.

    Rules are provided as JSON. Example rule:
    [{"metric":"num_failures","threshold":3,"action":{"kind":"set_version","target":1}}]
    """
    raw = _load_json(file, rules)
    wf_rules = [WorkflowRule.model_validate(r) for r in raw]
    cp_call(lambda cp: cp.set_workflow_lifecycle_policy(workflow_id, wf_rules))
    success(f"Workflow lifecycle policy set on {workflow_id}.")


@lifecycle_app.command("get-agent")
def lifecycle_get_agent(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
    agent_name: str = typer.Argument(..., help="Agent name"),
):
    """Show the armed lifecycle rules for an agent (empty if none is set)."""
    policy = cp_call(lambda cp: cp.get_agent_lifecycle_policy(workflow_id, agent_name))
    output(policy or {})


@lifecycle_app.command("get-workflow")
def lifecycle_get_workflow(
    workflow_id: str = typer.Argument(..., help="Workflow ID"),
):
    """Show the armed lifecycle rules for a workflow (empty if none is set)."""
    rules = cp_call(lambda cp: cp.get_workflow_lifecycle_policy(workflow_id))
    output([r.model_dump(mode="json") for r in rules])
