"""Governance policy types — the surface customers actually write.

Pydantic models with snake_case fields and typed action constructors.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field

# ── Runtime policy (hard caps, snapshotted at invoke time) ───────────────────


class ToolCallLimit(BaseModel):
    """A cap on how many times one tool may be called during an agent run.
    `max_calls=0` blocks it outright; omit the entry for no cap."""

    tool: str
    max_calls: int


class CapabilityCallLimit(BaseModel):
    """A cap on how many times an agent may do a *kind* of thing, however it does it.

    `capability="write"` covers `write_file`, `edit_file` and `delete` together, so the
    cap survives the agent switching tools — which per-tool caps don't: cap `write_file`
    and the agent reaches for `edit_file`.

    `read` and `write` are the filesystem vocabulary most harnesses use; `execute` and
    `spawn` cover shell and subagent spawning. Whatever runs the harness maps its tools
    onto these."""

    capability: str
    max_calls: int


class ToolFailureLimit(BaseModel):
    """A cap on how many times one tool may *fail* during an agent run. Exceeding it
    raises `ToolFailureLimitExceeded`, ending the run — a repeatedly-failing tool is
    a broken dependency, not a soft constraint.

    `max_failures=2` tolerates two failures and raises on the third; 0 raises on the
    first. Omit the entry for no cap."""

    tool: str
    max_failures: int


class RuntimePolicy(BaseModel):
    """Caps that travel with the operation, enforced worker-side during the agent loop.

    Everything above `custom` is enforced by this SDK. `custom` is not: it carries limits
    only the caller can enforce — a harness's own vocabulary, or a rule your handler
    applies itself — so that they are declared, stored and readable in one place instead
    of living in whatever code happened to construct the agent.
    """

    max_llm_calls: int = 0
    max_cost_usd: float = 0
    max_tokens_per_call: int = 0
    max_call_seconds: float = 0  # 0 = unset (no per-call timeout)
    tool_call_limits: list[ToolCallLimit] = Field(default_factory=list)
    tool_failure_limits: list[ToolFailureLimit] = Field(default_factory=list)
    capability_call_limits: list[CapabilityCallLimit] = Field(default_factory=list)
    model: str | None = None
    # Opaque to BoundFlow: stored, shipped with the operation, and handed back via
    # ctx.policy() — never validated or enforced. Nothing here binds unless the caller
    # makes it bind, which is the whole contract.
    custom: dict[str, Any] = Field(default_factory=dict)


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
