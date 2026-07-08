"""Async control-plane client — registration, workflow lifecycle, policies.

Port of BoundFlow.ControlPlane.ControlPlaneClient on grpc.aio. Async +
context-manager; policy methods take a list of rules directly (no wrapper).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from boundflow.v1 import agent_policy_pb2 as ap
from boundflow.v1 import lifecycle_pb2 as lc
from boundflow.v1 import lifecycle_pb2_grpc as lc_grpc
from boundflow.v1 import pricing_pb2 as pricing_pb
from boundflow.v1 import registration_pb2 as reg
from boundflow.v1 import registration_pb2_grpc as reg_grpc
from boundflow.v1 import workflow_pb2 as ri
from boundflow.v1 import tenant_group_pb2 as tg_pb
from boundflow.v1 import tenant_pb2 as tn_pb

from .errors import from_rpc_error
from .policies import (
    AgentRule, Cooldown, Pause, RuntimePolicy, SetVersion, WorkflowMetric, WorkflowRule,
)


@dataclass
class TenantGroup:
    id: str
    name: str


@dataclass
class Tenant:
    id: str
    name: str
    tenant_group_id: str


class InvokeMode(str, Enum):
    """How piled-up invokes are handled for a workflow (WorkflowConfig.invoke_mode)."""
    COALESCE = "coalesce"  # latest-wins: a newer invoke supersedes older pending ones
    QUEUE = "queue"        # fan-in: every invoke runs, drained oldest-first (FIFO)


@dataclass
class WorkflowConfig:
    version: int = 0
    invoke_timeout_seconds: int = 60
    repeat_every_seconds: int = 0
    triggerable: bool = True
    # max_queue_depth (0 = server default) bounds the queue-mode backlog; ignored in
    # coalesce mode.
    invoke_mode: InvokeMode = InvokeMode.COALESCE
    max_queue_depth: int = 0


@dataclass
class Workflow:
    id: str
    tenant_id: str
    config: WorkflowConfig


class LifecycleState(str, Enum):
    UNKNOWN = "unknown"
    CREATING = "creating"
    ACTIVE = "active"
    SCHEDULED = "scheduled"
    BLOCKED = "blocked"
    INVOKING = "invoking"
    AWAITING_APPROVAL = "awaiting_approval"
    DELETING = "deleting"
    DELETED = "deleted"
    INTERRUPTED = "interrupted"


class WorkflowState(str, Enum):
    UNSPECIFIED = "unspecified"
    ACTIVE = "active"
    PAUSED = "paused"
    COOLDOWN = "cooldown"
    DISABLED = "disabled"


class RunStatus(str, Enum):
    """Lifecycle of a single run (Run.status / RequestInfo.status)."""
    UNSCHEDULED = "unscheduled"
    SCHEDULED = "scheduled"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    COMPLETED = "completed"
    SUPERCEDED = "superceded"

    def is_terminal(self) -> bool:
        return self in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.SUPERCEDED)


class RunOutcome(str, Enum):
    """The terminal, customer-facing result of a run (None until the run is terminal)."""
    SUCCESSFUL = "successful"
    CUSTOMER_MARKED_FAILURE = "customer_marked_failure"
    UNCAUGHT_OPERATION_EXCEPTION = "uncaught_operation_exception"
    OPERATION_TIMEOUT = "operation_timeout"
    INTERRUPTED = "interrupted"


class ApprovalDecision(str, Enum):
    """How an approval gate was resolved (ApprovalAuditRecord.decision)."""
    UNSPECIFIED = "unspecified"
    APPROVED = "approved"
    REJECTED = "rejected"
    TIMED_OUT = "timed_out"


class WorkflowPolicyAction(str, Enum):
    """The action a workflow-lifecycle rule took (PolicyActionRecord.action)."""
    UNSPECIFIED = "unspecified"
    SET_VERSION = "set_version"
    COOLDOWN = "cooldown"
    PAUSE = "pause"


@dataclass
class WorkflowInfo:
    """A read-only view of a workflow — its current version, lifecycle state, and
    workflow state. Returned by `get_workflow` (one) and `list_workflows` (all)."""
    id: str
    workflow_type: str
    tenant_id: str
    lifecycle_state: LifecycleState
    workflow_state: WorkflowState
    version: int
    last_interrupted_request_id: str


def _workflow_info(w) -> WorkflowInfo:
    return WorkflowInfo(
        id=w.id,
        workflow_type=w.workflow_type,
        tenant_id=w.tenant_id,
        lifecycle_state=_LIFECYCLE.get(w.lifecycle_state, LifecycleState.UNKNOWN),
        workflow_state=_WF_STATE.get(w.workflow_state, WorkflowState.UNSPECIFIED),
        version=w.workflow_config.version,
        last_interrupted_request_id=w.last_interrupted_request_id,
    )


@dataclass
class Run:
    """One run (invocation) of a workflow. `status` is the run lifecycle
    (`RunStatus`); `run_outcome` is the terminal `RunOutcome`, or None while the run
    is still in flight. failure_reason carries detail (e.g. the exception text for an
    uncaught_operation_exception)."""
    request_id: str
    request_type: str
    status: RunStatus
    run_outcome: RunOutcome | None
    failure_reason: str
    created_at: datetime | None
    completed_at: datetime | None


@dataclass
class RequestInfo:
    """Full state of one run, from get_request_info(request_id). `status` is the run's
    lifecycle (`RunStatus`); `run_outcome` is the terminal `RunOutcome`, or None until
    the run is terminal. sequence_number orders a workflow's runs (monotonic per
    workflow)."""
    request_id: str
    workflow_id: str
    request_type: str
    status: RunStatus
    run_outcome: RunOutcome | None
    failure_reason: str
    sequence_number: int
    created_at: datetime | None
    completed_at: datetime | None


@dataclass
class ApprovalAuditRecord:
    """One approval decision from the audit log. Correlate with a run trace via
    approval_id (the trace's boundflow.approval_id) — the decision/actor/timing live
    here, not in telemetry."""
    workflow_id: str
    request_id: str
    approval_id: str
    decision: ApprovalDecision
    opened_at: datetime | None
    decided_at: datetime | None
    actor: str                     # customer-supplied; empty for timeouts
    occurred_at: datetime | None   # when the decision was recorded


@dataclass
class PolicyActionRecord:
    """One workflow-lifecycle policy firing. Self-describing: the rule that fired
    (identified by content), the value that crossed, and the prior state."""
    workflow_id: str
    request_id: str
    metric: str
    threshold: float
    window: int
    tool: str
    action: WorkflowPolicyAction
    target_version: int        # set_version
    cooldown_seconds: int      # cooldown
    trigger_value: float
    previous_version: int
    previous_state: str
    actor: str                 # "system"
    occurred_at: datetime | None


@dataclass
class AgentPolicyActionRecord:
    """One agent-lifecycle policy firing: the agent's effective runtime policy changed
    this run because rules fired. base_policy → effective_policy is the diff;
    fired_rules is why."""
    workflow_id: str
    request_id: str
    agent: str
    base_policy: dict          # {model, max_llm_calls, max_cost_usd, max_tokens_per_call, tool_call_limits}
    effective_policy: dict
    fired_rules: list[dict]    # [{metric, op, threshold, window, tool, action, trigger_value}]
    actor: str                 # "system"
    occurred_at: datetime | None


_APPROVAL_DECISION = {
    lc.APPROVAL_DECISION_APPROVED: ApprovalDecision.APPROVED,
    lc.APPROVAL_DECISION_REJECTED: ApprovalDecision.REJECTED,
    lc.APPROVAL_DECISION_TIMED_OUT: ApprovalDecision.TIMED_OUT,
}
_WORKFLOW_METRIC = {
    lc.WORKFLOW_METRIC_NUM_FAILURES: "num_failures",
    lc.WORKFLOW_METRIC_COST: "cost",
    lc.WORKFLOW_METRIC_NUM_LLM_CALLS: "num_llm_calls",
    lc.WORKFLOW_METRIC_LATENCY: "latency",
    lc.WORKFLOW_METRIC_APPROVAL_REJECTIONS: "approval_rejections",
    lc.WORKFLOW_METRIC_TOOL_FAILURE_RATE: "tool_failure_rate",
}
_WORKFLOW_ACTION = {
    lc.WORKFLOW_POLICY_ACTION_PAUSE: WorkflowPolicyAction.PAUSE,
    lc.WORKFLOW_POLICY_ACTION_COOLDOWN: WorkflowPolicyAction.COOLDOWN,
    lc.WORKFLOW_POLICY_ACTION_SET_VERSION: WorkflowPolicyAction.SET_VERSION,
}
_AGENT_METRIC = {
    ap.AGENT_METRIC_TOKENS_USED: "tokens_used",
    ap.AGENT_METRIC_COST_USD: "cost_usd",
    ap.AGENT_METRIC_LLM_CALLS: "llm_calls",
    ap.AGENT_METRIC_CALLS_PER_TOOL: "calls_per_tool",
}
_AGENT_OP = {
    ap.AGENT_OP_LT: "less_than",
    ap.AGENT_OP_LTE: "less_than_or_equal",
    ap.AGENT_OP_GT: "greater_than",
    ap.AGENT_OP_GTE: "greater_than_or_equal",
    ap.AGENT_OP_EQ: "equal",
}
_AGENT_ACTION_FIELD = {
    ap.AGENT_RULE_ACTION_SET_MODEL: "model",
    ap.AGENT_RULE_ACTION_SET_MAX_LLM_CALLS: "max_llm_calls",
    ap.AGENT_RULE_ACTION_SET_MAX_COST_USD: "max_cost_usd",
    ap.AGENT_RULE_ACTION_SET_MAX_TOKENS_PER_CALL: "max_tokens_per_call",
}


def _ts(msg, field):
    return getattr(msg, field).ToDatetime() if msg.HasField(field) else None


def _approval_record(r) -> ApprovalAuditRecord:
    return ApprovalAuditRecord(
        workflow_id=r.workflow_id, request_id=r.request_id, approval_id=r.approval_id,
        decision=_APPROVAL_DECISION.get(r.decision, ApprovalDecision.UNSPECIFIED),
        opened_at=_ts(r, "opened_at"), decided_at=_ts(r, "decided_at"),
        actor=r.actor, occurred_at=_ts(r, "occurred_at"))


def _workflow_policy_record(r) -> PolicyActionRecord:
    act = r.rule.action
    return PolicyActionRecord(
        workflow_id=r.workflow_id, request_id=r.request_id,
        metric=_WORKFLOW_METRIC.get(r.rule.metric, "unknown"), threshold=r.rule.threshold,
        window=r.rule.window, tool=r.rule.tool_name,
        action=_WORKFLOW_ACTION.get(act.type, WorkflowPolicyAction.UNSPECIFIED),
        target_version=act.target_version, cooldown_seconds=act.cooldown_seconds,
        trigger_value=r.trigger_value, previous_version=r.previous_version,
        previous_state=r.previous_state, actor=r.actor, occurred_at=_ts(r, "occurred_at"))


def _runtime_policy_dict(p) -> dict:
    return {
        "model": p.model, "max_llm_calls": p.max_llm_calls, "max_cost_usd": p.max_cost_usd,
        "max_tokens_per_call": p.max_tokens_per_call,
        "tool_call_limits": [{"tool": l.tool, "max_calls": l.max_calls} for l in p.tool_call_limits],
    }


def _agent_rule_dict(fr) -> dict:
    r, a = fr.rule, fr.rule.action
    field = _AGENT_ACTION_FIELD.get(a.field, "unknown")
    value = {"model": a.model, "max_llm_calls": a.max_llm_calls,
             "max_cost_usd": a.max_cost_usd, "max_tokens_per_call": a.max_tokens_per_call}.get(field)
    return {
        "metric": _AGENT_METRIC.get(r.metric, "unknown"), "op": _AGENT_OP.get(r.op, "unknown"),
        "threshold": r.threshold, "window": r.window, "tool": r.tool,
        "action": {"field": field, "value": value}, "trigger_value": fr.trigger_value,
    }


def _agent_policy_record(r) -> AgentPolicyActionRecord:
    a = r.action
    return AgentPolicyActionRecord(
        workflow_id=r.workflow_id, request_id=r.request_id, agent=r.agent_name,
        base_policy=_runtime_policy_dict(a.base_policy),
        effective_policy=_runtime_policy_dict(a.effective_policy),
        fired_rules=[_agent_rule_dict(fr) for fr in a.fired_rules],
        actor=r.actor, occurred_at=_ts(r, "occurred_at"))


_LIFECYCLE = {
    "creating": LifecycleState.CREATING,
    "active": LifecycleState.ACTIVE,
    "scheduled": LifecycleState.SCHEDULED,
    "blocked": LifecycleState.BLOCKED,
    "invoking": LifecycleState.INVOKING,
    "awaiting_approval": LifecycleState.AWAITING_APPROVAL,
    "deleting": LifecycleState.DELETING,
    "deleted": LifecycleState.DELETED,
    "interrupted": LifecycleState.INTERRUPTED,
}

_WF_STATE = {
    ri.WORKFLOW_STATE_ACTIVE: WorkflowState.ACTIVE,
    ri.WORKFLOW_STATE_PAUSED: WorkflowState.PAUSED,
    ri.WORKFLOW_STATE_COOLDOWN: WorkflowState.COOLDOWN,
    ri.WORKFLOW_STATE_DISABLED: WorkflowState.DISABLED,
}

_WF_METRIC = {
    WorkflowMetric.NUM_FAILURES: lc.WORKFLOW_METRIC_NUM_FAILURES,
    WorkflowMetric.COST: lc.WORKFLOW_METRIC_COST,
    WorkflowMetric.NUM_LLM_CALLS: lc.WORKFLOW_METRIC_NUM_LLM_CALLS,
    WorkflowMetric.LATENCY: lc.WORKFLOW_METRIC_LATENCY,
    WorkflowMetric.APPROVAL_REJECTIONS: lc.WORKFLOW_METRIC_APPROVAL_REJECTIONS,
    WorkflowMetric.TOOL_FAILURE_RATE: lc.WORKFLOW_METRIC_TOOL_FAILURE_RATE,
}
_WF_METRIC_REV = {v: k for k, v in _WF_METRIC.items()}


def _struct(d: dict) -> Struct:
    s = Struct()
    s.update(d)
    return s


def _strip(addr: str) -> str:
    return addr.split("://", 1)[1] if "://" in addr else addr


def _make_channel(addr: str):
    target = _strip(addr)
    if addr.startswith("https://"):
        return grpc.aio.secure_channel(target, grpc.ssl_channel_credentials())
    return grpc.aio.insecure_channel(target)


# Control-plane endpoint resolution order: explicit arg -> env -> self-host default.
DEFAULT_SERVER_ADDRESS = "http://localhost:50051"


class _TranslatingStub:
    """Wraps a gRPC stub so every unary call raises a typed BoundflowError instead of
    a raw grpc.aio.AioRpcError — callers never see the transport layer."""

    def __init__(self, stub):
        self._stub = stub

    def __getattr__(self, name):
        method = getattr(self._stub, name)

        async def call(*args, **kwargs):
            try:
                return await method(*args, **kwargs)
            except grpc.aio.AioRpcError as exc:
                raise from_rpc_error(exc) from exc

        return call


class ControlPlaneClient:
    def __init__(self, server_address: str | None = None, api_key: str | None = None) -> None:
        address = server_address or os.environ.get("BOUNDFLOW_SERVER_ADDRESS") or DEFAULT_SERVER_ADDRESS
        key = api_key or os.environ.get("BOUNDFLOW_API_KEY") or ""
        if not key:
            raise ValueError("api_key must be provided or BOUNDFLOW_API_KEY must be set")
        self._metadata = (("x-api-key", key),)
        self._channel = _make_channel(address)
        self._reg = _TranslatingStub(reg_grpc.RegistrationServiceStub(self._channel))
        self._lc = _TranslatingStub(lc_grpc.WorkflowServiceStub(self._channel))

    async def __aenter__(self) -> "ControlPlaneClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def close(self) -> None:
        await self._channel.close()

    # ── Tenant groups & tenants ──────────────────────────────────────────────

    async def create_tenant_group(self, name: str) -> TenantGroup:
        resp = await self._reg.CreateTenantGroup(
            reg.CreateTenantGroupRequest(tenant_group=tg_pb.TenantGroup(name=name)),
            metadata=self._metadata)
        return TenantGroup(resp.tenant_group.id, resp.tenant_group.name)

    async def create_tenant(self, name: str) -> Tenant:
        resp = await self._reg.CreateTenant(
            reg.CreateTenantRequest(tenant=tn_pb.Tenant(name=name)),
            metadata=self._metadata)
        return Tenant(resp.tenant.id, resp.tenant.name, resp.tenant.tenant_group_id)

    async def get_tenant_group(self, tenant_group_id: str) -> TenantGroup:
        resp = await self._reg.GetTenantGroup(
            reg.GetTenantGroupRequest(id=tenant_group_id),
            metadata=self._metadata)
        return TenantGroup(resp.tenant_group.id, resp.tenant_group.name)

    async def delete_tenant_group(self, tenant_group_id: str) -> None:
        await self._reg.DeleteTenantGroup(
            reg.DeleteTenantGroupRequest(id=tenant_group_id),
            metadata=self._metadata)

    async def get_tenant(self, tenant_id: str) -> Tenant:
        resp = await self._reg.GetTenant(
            reg.GetTenantRequest(id=tenant_id),
            metadata=self._metadata)
        return Tenant(resp.tenant.id, resp.tenant.name, resp.tenant.tenant_group_id)

    async def delete_tenant(self, tenant_id: str) -> None:
        await self._reg.DeleteTenant(
            reg.DeleteTenantRequest(id=tenant_id),
            metadata=self._metadata)

    async def list_tenants(self) -> list[Tenant]:
        """The caller's tenants, scoped to their tenant group (resolved from the API key)."""
        resp = await self._reg.ListTenants(
            reg.ListTenantsRequest(), metadata=self._metadata)
        return [Tenant(t.id, t.name, t.tenant_group_id) for t in resp.tenants]

    # ── Pricing ──────────────────────────────────────────────────────────────

    async def set_model_pricing(self, model_id: str, input_per_1m: float, output_per_1m: float) -> None:
        """Override this tenant group's per-1M-token rates (USD) for a model."""
        await self._reg.SetModelPricing(
            reg.SetModelPricingRequest(pricing=pricing_pb.ModelPricing(
                model_id=model_id, input_per_1m=input_per_1m, output_per_1m=output_per_1m)),
            metadata=self._metadata)

    async def list_model_pricing(self) -> dict[str, dict[str, float]]:
        """Effective rates for this tenant group (defaults merged with overrides),
        as {model_id: {"input_per_1m", "output_per_1m"}}."""
        resp = await self._reg.ListModelPricing(
            reg.ListModelPricingRequest(), metadata=self._metadata)
        return {p.model_id: {"input_per_1m": p.input_per_1m, "output_per_1m": p.output_per_1m}
                for p in resp.pricing}

    # ── Workflows ────────────────────────────────────────────────────────────

    async def create_workflow(
        self, workflow_type: str, tenant_id: str, config: WorkflowConfig | None = None
    ) -> Workflow:
        cfg = config or WorkflowConfig()
        resp = await self._lc.CreateWorkflow(lc.CreateWorkflowRequest(
            workflow_type=workflow_type,
            tenant_id=tenant_id,
            workflow_config=ri.WorkflowConfig(
                version=cfg.version,
                invoke_timeout_seconds=cfg.invoke_timeout_seconds,
                repeat_every_seconds=cfg.repeat_every_seconds,
                triggerable=cfg.triggerable,
                invoke_mode=(ri.INVOKE_MODE_QUEUE if cfg.invoke_mode == InvokeMode.QUEUE
                             else ri.INVOKE_MODE_COALESCE),
                max_queue_depth=cfg.max_queue_depth,
            ),
        ), metadata=self._metadata)
        inst = resp.workflow
        wc = inst.workflow_config
        return Workflow(inst.id, inst.tenant_id, WorkflowConfig(
            wc.version, wc.invoke_timeout_seconds, wc.repeat_every_seconds, wc.triggerable,
            InvokeMode.QUEUE if wc.invoke_mode == ri.INVOKE_MODE_QUEUE else InvokeMode.COALESCE,
            wc.max_queue_depth))

    async def activate_workflow(self, workflow_id: str) -> None:
        await self._lc.ActivateWorkflow(
            lc.ActivateWorkflowRequest(workflow_id=workflow_id),
            metadata=self._metadata)

    async def resolve_interrupted_workflow(self, workflow_id: str, request_id: str) -> None:
        """Resolve an interrupted workflow back to active. request_id must match the
        workflow's last_interrupted_request_id (the run that interrupted it) — read it
        from the workflow's last_interrupted_request_id field."""
        await self._lc.ResolveInterruptedWorkflow(
            lc.ResolveInterruptedWorkflowRequest(workflow_id=workflow_id, request_id=request_id),
            metadata=self._metadata)

    async def invoke_workflow(self, workflow_id: str, *, operation_timeout_seconds: int = 0) -> str:
        """Trigger a run; returns the request_id — the run/trace id you can use to
        find this invocation's trace later."""
        resp = await self._lc.InvokeWorkflow(lc.InvokeWorkflowRequest(
            workflow_id=workflow_id,
            runtime_overrides=lc.RuntimeOverrides(operation_timeout_seconds=operation_timeout_seconds),
        ), metadata=self._metadata)
        return resp.request_id

    async def get_workflow(self, workflow_id: str) -> WorkflowInfo:
        """Fetch one workflow by id — its current version, lifecycle state, and
        workflow state. The single-resource read; `list_workflows` returns all."""
        resp = await self._lc.GetWorkflow(
            lc.GetWorkflowRequest(workflow_id=workflow_id), metadata=self._metadata)
        return _workflow_info(resp.workflow)

    async def list_workflows(self) -> list[WorkflowInfo]:
        """List all workflows owned by this API key's tenant group (newest first)."""
        resp = await self._lc.ListWorkflows(
            lc.ListWorkflowsRequest(), metadata=self._metadata)
        return [_workflow_info(w) for w in resp.workflows]

    async def list_workflow_runs(self, workflow_id: str) -> list[Run]:
        """List a workflow's runs (invocations), newest first, with each run's outcome
        and failure reason."""
        resp = await self._lc.ListWorkflowRuns(
            lc.ListWorkflowRunsRequest(workflow_id=workflow_id), metadata=self._metadata)
        return [
            Run(
                request_id=r.request_id,
                request_type=r.request_type,
                status=RunStatus(r.status),
                run_outcome=RunOutcome(r.run_outcome) if r.run_outcome else None,
                failure_reason=r.failure_reason,
                created_at=_ts(r, "created_at"),
                completed_at=_ts(r, "completed_at"),
            )
            for r in resp.runs
        ]

    async def get_request_info(self, request_id: str) -> RequestInfo:
        """Get the full state of a single run by its request id (returned by invoke)."""
        resp = await self._lc.GetRequestInfo(
            lc.GetRequestInfoRequest(request_id=request_id), metadata=self._metadata)
        r = resp.request
        return RequestInfo(
            request_id=r.request_id,
            workflow_id=r.workflow_id,
            request_type=r.request_type,
            status=RunStatus(r.status),
            run_outcome=RunOutcome(r.run_outcome) if r.run_outcome else None,
            failure_reason=r.failure_reason,
            sequence_number=r.version,
            created_at=_ts(r, "created_at"),
            completed_at=_ts(r, "completed_at"),
        )

    async def approve_workflow(self, workflow_id: str, approval_id: str, actor: str = "") -> None:
        """Approve a parked gate. `actor` identifies the approver (e.g. an email or
        user id); it's recorded in the approval audit log (auth is tenant-scoped, so
        the customer's gate is the source of approver identity)."""
        await self._lc.ApproveWorkflow(
            lc.ApproveWorkflowRequest(workflow_id=workflow_id, approval_id=approval_id, actor=actor),
            metadata=self._metadata)

    async def reject_workflow(self, workflow_id: str, approval_id: str, actor: str = "") -> None:
        await self._lc.RejectWorkflow(
            lc.RejectWorkflowRequest(workflow_id=workflow_id, approval_id=approval_id, actor=actor),
            metadata=self._metadata)

    async def get_approval_audit(self, workflow_id: str) -> list[ApprovalAuditRecord]:
        """A workflow's approval decisions (newest first)."""
        resp = await self._lc.GetApprovalAudit(
            lc.GetApprovalAuditRequest(workflow_id=workflow_id), metadata=self._metadata)
        return [_approval_record(r) for r in resp.records]

    async def get_approval_audit_by_id(self, approval_id: str) -> ApprovalAuditRecord | None:
        """The single approval decision for an approval_id (the trace's correlation
        key), or None if not found."""
        resp = await self._lc.GetApprovalAuditById(
            lc.GetApprovalAuditByIdRequest(approval_id=approval_id), metadata=self._metadata)
        return _approval_record(resp.record) if resp.HasField("record") else None

    async def get_workflow_policy_audit(self, workflow_id: str) -> list[PolicyActionRecord]:
        """A workflow's workflow-lifecycle policy firings (newest first)."""
        resp = await self._lc.GetWorkflowPolicyAudit(
            lc.GetWorkflowPolicyAuditRequest(workflow_id=workflow_id), metadata=self._metadata)
        return [_workflow_policy_record(r) for r in resp.records]

    async def get_agent_policy_audit(self, workflow_id: str, agent_name: str) -> list[AgentPolicyActionRecord]:
        """A specific agent's lifecycle policy firings (newest first). Agents are
        identified by (workflow_id, agent_name)."""
        resp = await self._lc.GetAgentPolicyAudit(
            lc.GetAgentPolicyAuditRequest(workflow_id=workflow_id, agent_name=agent_name),
            metadata=self._metadata)
        return [_agent_policy_record(r) for r in resp.records]

    async def get_audit_log(self, workflow_id: str = ""):
        """The unified, time-ordered audit log (newest first). workflow_id is optional
        — omit for the whole tenant group. Each item is an ApprovalAuditRecord,
        PolicyActionRecord, or AgentPolicyActionRecord."""
        resp = await self._lc.GetAuditLog(
            lc.GetAuditLogRequest(workflow_id=workflow_id), metadata=self._metadata)
        out = []
        for e in resp.entries:
            which = e.WhichOneof("entry")
            if which == "approval":
                out.append(_approval_record(e.approval))
            elif which == "workflow_policy":
                out.append(_workflow_policy_record(e.workflow_policy))
            elif which == "agent_policy":
                out.append(_agent_policy_record(e.agent_policy))
        return out

    async def delete_workflow(self, workflow_id: str) -> None:
        await self._lc.DeleteWorkflow(
            lc.DeleteWorkflowRequest(workflow_id=workflow_id),
            metadata=self._metadata)

    # ── Policies ─────────────────────────────────────────────────────────────

    async def set_agent_runtime_policy(
        self, workflow_id: str, agent_name: str, policy: RuntimePolicy
    ) -> None:
        await self._lc.SetAgentRuntimePolicy(lc.SetAgentRuntimePolicyRequest(
            workflow_id=workflow_id,
            agent_name=agent_name,
            runtime_policy=_struct(policy.model_dump(mode="json", exclude_none=True)),
        ), metadata=self._metadata)

    async def set_agent_lifecycle_policy(
        self, workflow_id: str, agent_name: str, rules: list[AgentRule]
    ) -> None:
        payload = {"rules": [r.model_dump(mode="json", exclude_none=True) for r in rules]}
        await self._lc.SetAgentLifecyclePolicy(lc.SetAgentLifecyclePolicyRequest(
            workflow_id=workflow_id,
            agent_name=agent_name,
            lifecycle_policy=_struct(payload),
        ), metadata=self._metadata)

    async def set_workflow_lifecycle_policy(
        self, workflow_id: str, rules: list[WorkflowRule]
    ) -> None:
        await self._lc.SetWorkflowLifecyclePolicy(lc.SetWorkflowLifecyclePolicyRequest(
            workflow_id=workflow_id,
            lifecycle_policy=lc.WorkflowLifecyclePolicy(
                rules=[_workflow_rule_proto(r) for r in rules]),
        ), metadata=self._metadata)

    async def get_workflow_lifecycle_policy(self, workflow_id: str) -> list[WorkflowRule]:
        """The armed workflow-lifecycle policy — the rules currently configured (the
        inverse of set_workflow_lifecycle_policy). Empty list if none is set. Reflects the
        current config, not a per-run snapshot; capture at run time for an audit receipt."""
        resp = await self._lc.GetWorkflowLifecyclePolicy(
            lc.GetWorkflowLifecyclePolicyRequest(workflow_id=workflow_id),
            metadata=self._metadata)
        return [_workflow_rule_from_proto(r) for r in resp.lifecycle_policy.rules]

    async def get_agent_runtime_policy(self, workflow_id: str, agent_name: str) -> dict:
        """The armed runtime policy (hard caps + model override) for one agent, as a dict.
        Empty dict if none is set."""
        resp = await self._lc.GetAgentRuntimePolicy(
            lc.GetAgentRuntimePolicyRequest(workflow_id=workflow_id, agent_name=agent_name),
            metadata=self._metadata)
        return MessageToDict(resp.runtime_policy)

    async def get_agent_lifecycle_policy(self, workflow_id: str, agent_name: str) -> dict:
        """The armed lifecycle policy (adaptive rules) for one agent, as a dict. Empty dict
        if none is set."""
        resp = await self._lc.GetAgentLifecyclePolicy(
            lc.GetAgentLifecyclePolicyRequest(workflow_id=workflow_id, agent_name=agent_name),
            metadata=self._metadata)
        return MessageToDict(resp.lifecycle_policy)


def _workflow_rule_proto(rule: WorkflowRule) -> lc.WorkflowLifecyclePolicyRule:
    action = rule.action
    if isinstance(action, Pause):
        act = lc.WorkflowLifecyclePolicyAction(type=lc.WORKFLOW_POLICY_ACTION_PAUSE)
        window = action.window
    elif isinstance(action, Cooldown):
        act = lc.WorkflowLifecyclePolicyAction(
            type=lc.WORKFLOW_POLICY_ACTION_COOLDOWN, cooldown_seconds=action.seconds)
        window = action.window
    elif isinstance(action, SetVersion):
        act = lc.WorkflowLifecyclePolicyAction(
            type=lc.WORKFLOW_POLICY_ACTION_SET_VERSION, target_version=action.target)
        window = 0
    else:
        raise ValueError(f"Unknown workflow action: {type(action).__name__}")
    return lc.WorkflowLifecyclePolicyRule(
        metric=_WF_METRIC[rule.metric],
        threshold=rule.threshold,
        window=window,
        tool_name=rule.tool or "",
        action=act,
    )


def _workflow_rule_from_proto(r: lc.WorkflowLifecyclePolicyRule) -> WorkflowRule:
    """Inverse of _workflow_rule_proto: proto rule → WorkflowRule. The rule's window
    lives on the action in the SDK model (Pause/Cooldown), so it's folded back in."""
    t = r.action.type
    if t == lc.WORKFLOW_POLICY_ACTION_COOLDOWN:
        action = Cooldown(window=r.window, seconds=r.action.cooldown_seconds)
    elif t == lc.WORKFLOW_POLICY_ACTION_SET_VERSION:
        action = SetVersion(target=r.action.target_version)
    else:
        action = Pause(window=r.window)
    return WorkflowRule(
        metric=_WF_METRIC_REV[r.metric],
        threshold=r.threshold,
        action=action,
        tool=r.tool_name or None,
    )
