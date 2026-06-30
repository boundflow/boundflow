"""bfc pricing — model pricing management."""

import typer

from boundflow.cli._client import cp_call
from boundflow.cli._output import output, success

app = typer.Typer(help="Manage model pricing rates.")


@app.command("list")
def list_pricing():
    """List all model pricing rates (USD per 1M tokens)."""
    result = cp_call(lambda cp: cp.list_model_pricing())
    rows = [
        {"model": model, "input_per_1m_usd": rates["input_per_1m"], "output_per_1m_usd": rates["output_per_1m"]}
        for model, rates in result.items()
    ]
    output(rows)


@app.command("set")
def set_pricing(
    model_id: str = typer.Argument(..., help="Model identifier (e.g. claude-sonnet-4-6)"),
    input_rate: float = typer.Option(..., "--input", help="Input token rate (USD per 1M tokens)"),
    output_rate: float = typer.Option(..., "--output", help="Output token rate (USD per 1M tokens)"),
):
    """Set or override pricing for a model."""
    cp_call(lambda cp: cp.set_model_pricing(model_id, input_rate, output_rate))
    success(f"Pricing set for {model_id}: ${input_rate}/1M input, ${output_rate}/1M output.")
