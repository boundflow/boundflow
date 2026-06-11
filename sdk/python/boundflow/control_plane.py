"""Async control-plane client — registration, workflow lifecycle, policies.

Port of BoundFlow.ControlPlane.ControlPlaneClient on grpc.aio. Async +
context-manager; policy methods take a list of rules directly (no wrapper).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum

import grpc
from google.protobuf.struct_pb2 import Struct

from convergeplane.v1 import lifecycle_pb2 as lc
from convergeplane.v1 import lifecycle_pb2_grpc as lc_grpc
from convergeplane.v1 import registration_pb2 as reg
from convergeplane.v1 import registration_pb2_grpc as reg_grpc
from convergeplane.v1 import resource_instance_pb2 as ri
from convergeplane.v1 import tenant_group_pb2 as tg_pb
from convergeplane.v1 import tenant_pb2 as tn_pb

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


@dataclass
class WorkflowConfig:
    version: int = 0
    invoke_timeout_seconds: int = 60
    repeat_every_seconds: int = 0
    triggerable: bool = True


@dataclass
class Workflow:
    id: str
    tenant_id: str
    config: WorkflowConfig


class LifecycleState(str, Enum):
    UNKNOWN = "unknown"
    CREATING = "creating"
    ACTIVE = "active"
    INVOKING = "invoking"
    AWAITING_APPROVAL = "awaiting_approval"
    DELETING = "deleting"
    DELETED = "deleted"
    FAILED = "failed"


class WorkflowState(str, Enum):
    UNSPECIFIED = "unspecified"
    ACTIVE = "active"
    PAUSED = "paused"
    COOLDOWN = "cooldown"
    DISABLED = "disabled"


_LIFECYCLE = {
    "creating": LifecycleState.CREATING,
    "active": LifecycleState.ACTIVE,
    "reconciling": LifecycleState.INVOKING,
    "awaiting_approval": LifecycleState.AWAITING_APPROVAL,
    "deleting": LifecycleState.DELETING,
    "deleted": LifecycleState.DELETED,
    "failed": LifecycleState.FAILED,
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


def _struct(d: dict) -> Struct:
    s = Struct()
    s.update(d)
    return s


def _strip(addr: str) -> str:
    return addr.split("://", 1)[1] if "://" in addr else addr


class ControlPlaneClient:
    def __init__(self, server_address: str, api_key: str | None = None) -> None:
        key = api_key or os.environ.get("BOUNDFLOW_API_KEY") or ""
        if not key:
            raise ValueError("api_key must be provided or BOUNDFLOW_API_KEY must be set")
        self._metadata = (("x-api-key", key),)
        self._channel = grpc.aio.insecure_channel(_strip(server_address))
        self._reg = reg_grpc.RegistrationServiceStub(self._channel)
        self._lc = lc_grpc.ResourceLifecycleServiceStub(self._channel)

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

    # ── Workflows ────────────────────────────────────────────────────────────

    async def create_workflow(
        self, workflow_type: str, tenant_id: str, config: WorkflowConfig | None = None
    ) -> Workflow:
        cfg = config or WorkflowConfig()
        resp = await self._lc.CreateResource(lc.CreateResourceRequest(
            resource_type=workflow_type,
            tenant_id=tenant_id,
            workflow_config=ri.WorkflowConfig(
                version=cfg.version,
                invoke_timeout_seconds=cfg.invoke_timeout_seconds,
                repeat_every_seconds=cfg.repeat_every_seconds,
                triggerable=cfg.triggerable,
            ),
        ), metadata=self._metadata)
        inst = resp.resource_instance
        wc = inst.workflow_config
        return Workflow(inst.id, inst.tenant_id, WorkflowConfig(
            wc.version, wc.invoke_timeout_seconds, wc.repeat_every_seconds, wc.triggerable))

    async def activate_workflow(self, workflow_id: str) -> None:
        await self._lc.ActivateWorkflow(
            lc.ActivateWorkflowRequest(resource_instance_id=workflow_id),
            metadata=self._metadata)

    async def invoke_workflow(self, workflow_id: str, *, operation_timeout_seconds: int = 0) -> None:
        await self._lc.ReconcileResource(lc.ReconcileResourceRequest(
            resource_instance_id=workflow_id,
            runtime_overrides=lc.RuntimeOverrides(operation_timeout_seconds=operation_timeout_seconds),
        ), metadata=self._metadata)

    async def get_workflow_state(self, workflow_id: str) -> WorkflowState | None:
        resp = await self._lc.GetResourceState(
            lc.GetResourceStateRequest(resource_instance_id=workflow_id),
            metadata=self._metadata)
        inst = resp.resource_instance
        if inst.lifecycle_state == "deleted":
            return None
        return _WF_STATE.get(inst.workflow_state, WorkflowState.UNSPECIFIED)

    async def get_workflow_lifecycle_state(self, workflow_id: str) -> LifecycleState:
        resp = await self._lc.GetResourceState(
            lc.GetResourceStateRequest(resource_instance_id=workflow_id),
            metadata=self._metadata)
        return _LIFECYCLE.get(resp.resource_instance.lifecycle_state, LifecycleState.UNKNOWN)

    async def approve_workflow(self, workflow_id: str, approval_id: str) -> None:
        await self._lc.ApproveWorkflow(
            lc.ApproveWorkflowRequest(resource_instance_id=workflow_id, approval_id=approval_id),
            metadata=self._metadata)

    async def reject_workflow(self, workflow_id: str, approval_id: str) -> None:
        await self._lc.RejectWorkflow(
            lc.RejectWorkflowRequest(resource_instance_id=workflow_id, approval_id=approval_id),
            metadata=self._metadata)

    async def delete_workflow(self, workflow_id: str) -> None:
        await self._lc.DeleteResource(
            lc.DeleteResourceRequest(resource_instance_id=workflow_id),
            metadata=self._metadata)

    # ── Policies ─────────────────────────────────────────────────────────────

    async def set_agent_runtime_policy(
        self, workflow_id: str, agent_name: str, policy: RuntimePolicy
    ) -> None:
        await self._lc.SetAgentRuntimePolicy(lc.SetAgentRuntimePolicyRequest(
            resource_instance_id=workflow_id,
            agent_name=agent_name,
            runtime_policy=_struct(policy.model_dump(mode="json", exclude_none=True)),
        ), metadata=self._metadata)

    async def set_agent_lifecycle_policy(
        self, workflow_id: str, agent_name: str, rules: list[AgentRule]
    ) -> None:
        payload = {"rules": [r.model_dump(mode="json", exclude_none=True) for r in rules]}
        await self._lc.SetAgentLifecyclePolicy(lc.SetAgentLifecyclePolicyRequest(
            resource_instance_id=workflow_id,
            agent_name=agent_name,
            lifecycle_policy=_struct(payload),
        ), metadata=self._metadata)

    async def set_workflow_lifecycle_policy(
        self, workflow_id: str, rules: list[WorkflowRule]
    ) -> None:
        await self._lc.SetWorkflowLifecyclePolicy(lc.SetWorkflowLifecyclePolicyRequest(
            resource_instance_id=workflow_id,
            lifecycle_policy=lc.WorkflowLifecyclePolicy(
                rules=[_workflow_rule_proto(r) for r in rules]),
        ), metadata=self._metadata)


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
