"""Governance policy types — the surface customers actually write.

Pydantic models with snake_case fields and typed action constructors.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field

# ── Runtime policy (hard caps, snapshotted at invoke time) ───────────────────


class ToolCallLimit(BaseModel):
    """A cap on how many times one tool may be called during an agent run."""

    tool: str
    max_calls: int


class RuntimePolicy(BaseModel):
    """Hard caps enforced SDK-side during the agent loop."""

    max_llm_calls: int = 0
    max_cost_usd: float = 0
    max_tokens_per_call: int = 0
    max_call_seconds: float = 0  # 0 = unset (no per-call timeout)
    tool_call_limits: list[ToolCallLimit] = Field(default_factory=list)
    model: str | None = None


# ── Agent lifecycle policy (reacts to prior-run metrics) ─────────────────────


class AgentMetric(str, Enum):
    """A per-agent metric that agent-lifecycle rules evaluate."""

    TOKENS_USED = "tokens_used"
    COST_USD = "cost_usd"
    LLM_CALLS = "llm_calls"
    CALLS_PER_TOOL = "calls_per_tool"


class Op(str, Enum):
    """Comparison operator for a rule's threshold check."""

    LT = "less_than"
    LTE = "less_than_or_equal"
    GT = "greater_than"
    GTE = "greater_than_or_equal"
    EQ = "equal"


# Actions: typed constructors. `SetModel(OPUS)` reads far better than an
# untyped (field, value) pair.


class SetModel(BaseModel):
    """Agent-lifecycle action: switch the agent to a different model."""

    field: Literal["model"] = "model"
    value: str


class SetMaxLlmCalls(BaseModel):
    """Agent-lifecycle action: change the agent's max-LLM-calls cap."""

    field: Literal["max_llm_calls"] = "max_llm_calls"
    value: int


class SetMaxCostUsd(BaseModel):
    """Agent-lifecycle action: change the agent's max-cost cap."""

    field: Literal["max_cost_usd"] = "max_cost_usd"
    value: float


class SetMaxTokensPerCall(BaseModel):
    """Agent-lifecycle action: change the agent's max-tokens-per-call cap."""

    field: Literal["max_tokens_per_call"] = "max_tokens_per_call"
    value: int


AgentAction = Annotated[
    Union[SetModel, SetMaxLlmCalls, SetMaxCostUsd, SetMaxTokensPerCall],
    Field(discriminator="field"),
]


class AgentRule(BaseModel):
    """When an agent metric crosses a threshold over a window of recent runs,
    apply an action to the agent's runtime policy."""

    metric: AgentMetric
    op: Op
    threshold: float
    window: int
    action: AgentAction
    # Only used when metric == CALLS_PER_TOOL: which tool's count to evaluate.
    tool: str | None = None


# ── Workflow lifecycle policy (reacts to workflow-level metrics) ─────────────


class WorkflowMetric(str, Enum):
    """A workflow-level metric that workflow-lifecycle rules evaluate."""

    NUM_FAILURES = "num_failures"
    COST = "cost"
    NUM_LLM_CALLS = "num_llm_calls"
    LATENCY = "latency"
    APPROVAL_REJECTIONS = "approval_rejections"
    TOOL_FAILURE_RATE = "tool_failure_rate"


class Pause(BaseModel):
    """Workflow-lifecycle action: pause the workflow, holding new runs until resumed."""

    kind: Literal["pause"] = "pause"
    window: int


class Cooldown(BaseModel):
    """Workflow-lifecycle action: pause the workflow, then auto-resume after `seconds`."""

    kind: Literal["cooldown"] = "cooldown"
    window: int
    seconds: int


class SetVersion(BaseModel):
    """Workflow-lifecycle action: roll the workflow to a target version."""

    kind: Literal["set_version"] = "set_version"
    target: int


WorkflowAction = Annotated[
    Union[Pause, Cooldown, SetVersion], Field(discriminator="kind")
]


class WorkflowRule(BaseModel):
    """When a workflow-level metric crosses a threshold, apply an action to the
    workflow (pause, cooldown, or roll to a version)."""

    metric: WorkflowMetric
    threshold: float
    action: WorkflowAction
    tool: str | None = None
