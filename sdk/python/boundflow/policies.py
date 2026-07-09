"""Governance policy types — the surface customers actually write.

Mirrors the .NET BoundFlow.ControlPlane policy records, made Python-native:
Pydantic models, snake_case, and typed action constructors instead of the
C# (PolicyField, value) pair.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field

# ── Runtime policy (hard caps, snapshotted at invoke time) ───────────────────


class ToolCallLimit(BaseModel):
    tool: str
    max_calls: int


class RuntimePolicy(BaseModel):
    """Hard caps enforced SDK-side during the agent loop. Unlike the other caps —
    which force a graceful submit_result on the next turn — max_call_seconds cancels
    an in-flight LLM call outright, since a hung call never reaches a next turn."""

    max_llm_calls: int = 0
    max_cost_usd: float = 0
    max_tokens_per_call: int = 0
    max_call_seconds: float = 0  # 0 = unset (no per-call timeout)
    tool_call_limits: list[ToolCallLimit] = Field(default_factory=list)
    model: str | None = None


# ── Agent lifecycle policy (reacts to prior-run metrics) ─────────────────────


class AgentMetric(str, Enum):
    TOKENS_USED = "tokens_used"
    COST_USD = "cost_usd"
    LLM_CALLS = "llm_calls"
    CALLS_PER_TOOL = "calls_per_tool"


class Op(str, Enum):
    LT = "less_than"
    LTE = "less_than_or_equal"
    GT = "greater_than"
    GTE = "greater_than_or_equal"
    EQ = "equal"


# Actions: typed constructors. `SetModel(OPUS)` reads far better than the C#
# `PolicyMutation(PolicyField.Model, OpusModel)`.


class SetModel(BaseModel):
    field: Literal["model"] = "model"
    value: str


class SetMaxLlmCalls(BaseModel):
    field: Literal["max_llm_calls"] = "max_llm_calls"
    value: int


class SetMaxCostUsd(BaseModel):
    field: Literal["max_cost_usd"] = "max_cost_usd"
    value: float


class SetMaxTokensPerCall(BaseModel):
    field: Literal["max_tokens_per_call"] = "max_tokens_per_call"
    value: int


AgentAction = Annotated[
    Union[SetModel, SetMaxLlmCalls, SetMaxCostUsd, SetMaxTokensPerCall],
    Field(discriminator="field"),
]


class AgentRule(BaseModel):
    metric: AgentMetric
    op: Op
    threshold: float
    window: int
    action: AgentAction
    # Only used when metric == CALLS_PER_TOOL: which tool's count to evaluate.
    tool: str | None = None


# ── Workflow lifecycle policy (reacts to workflow-level metrics) ─────────────


class WorkflowMetric(str, Enum):
    NUM_FAILURES = "num_failures"
    COST = "cost"
    NUM_LLM_CALLS = "num_llm_calls"
    LATENCY = "latency"
    APPROVAL_REJECTIONS = "approval_rejections"
    TOOL_FAILURE_RATE = "tool_failure_rate"


class Pause(BaseModel):
    kind: Literal["pause"] = "pause"
    window: int


class Cooldown(BaseModel):
    kind: Literal["cooldown"] = "cooldown"
    window: int
    seconds: int


class SetVersion(BaseModel):
    kind: Literal["set_version"] = "set_version"
    target: int


WorkflowAction = Annotated[
    Union[Pause, Cooldown, SetVersion], Field(discriminator="kind")
]


class WorkflowRule(BaseModel):
    metric: WorkflowMetric
    threshold: float
    action: WorkflowAction
    tool: str | None = None
