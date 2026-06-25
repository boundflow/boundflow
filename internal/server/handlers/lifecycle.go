package handlers

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/auth"
	"github.com/boundflow/boundflow/internal/convert"
	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/service"
	"github.com/boundflow/boundflow/internal/storage"
)

type ResourceLifecycleHandler struct {
	boundflowv1.UnimplementedResourceLifecycleServiceServer
	svc *service.LifecycleService
}

func NewResourceLifecycleHandler(svc *service.LifecycleService) *ResourceLifecycleHandler {
	return &ResourceLifecycleHandler{svc: svc}
}

// checkResourceOwner verifies the caller owns the given resource instance.
// Returns NotFound (not PermissionDenied) on failure to avoid leaking existence.
func (h *ResourceLifecycleHandler) checkResourceOwner(ctx context.Context, resourceInstanceID string) error {
	callerGroup, ok := auth.TenantGroupFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing auth")
	}
	ownerGroup, err := h.svc.TenantGroupIDForResource(ctx, resourceInstanceID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return status.Error(codes.NotFound, "resource instance not found")
		}
		return status.Errorf(codes.Internal, "check resource owner: %v", err)
	}
	if ownerGroup != callerGroup {
		return status.Error(codes.NotFound, "resource instance not found")
	}
	return nil
}

// checkTenantOwner verifies the caller owns the given tenant.
func (h *ResourceLifecycleHandler) checkTenantOwner(ctx context.Context, tenantID string) error {
	callerGroup, ok := auth.TenantGroupFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing auth")
	}
	ownerGroup, err := h.svc.TenantGroupIDForTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return status.Error(codes.NotFound, "tenant not found")
		}
		return status.Errorf(codes.Internal, "check tenant owner: %v", err)
	}
	if ownerGroup != callerGroup {
		return status.Error(codes.NotFound, "tenant not found")
	}
	return nil
}

func (h *ResourceLifecycleHandler) CreateResource(ctx context.Context, req *boundflowv1.CreateResourceRequest) (*boundflowv1.CreateResourceResponse, error) {
	if req.ResourceType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_type is required")
	}
	if req.TenantId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tenant_id is required")
	}

	if err := h.checkTenantOwner(ctx, req.TenantId); err != nil {
		return nil, err
	}

	cfg := convert.WorkflowConfigFromProto(req.WorkflowConfig)
	version := 0
	if req.WorkflowConfig != nil {
		version = int(req.WorkflowConfig.Version)
	}

	instance, err := h.svc.CreateResource(ctx, req.CorrelationId, req.ResourceType, req.TenantId, cfg, version)
	if err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "resource instance already exists")
		}
		return nil, status.Errorf(codes.Internal, "create resource: %v", err)
	}

	return &boundflowv1.CreateResourceResponse{
		ResourceInstance: convert.ResourceInstanceToProto(instance),
	}, nil
}

func (h *ResourceLifecycleHandler) ReconcileResource(ctx context.Context, req *boundflowv1.ReconcileResourceRequest) (*boundflowv1.ReconcileResourceResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	var params domain.WorkflowRuntimeParams
	if req.RuntimeOverrides != nil {
		params.OperationTimeoutSeconds = int(req.RuntimeOverrides.OperationTimeoutSeconds)
	}

	if err := h.svc.ReconcileResource(ctx, req.CorrelationId, req.ResourceInstanceId, params); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "resource instance not found")
		}
		if errors.Is(err, storage.ErrInvalidLifecycleState) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		if errors.Is(err, service.ErrMissingRuntimeParams) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		if errors.Is(err, service.ErrInvalidWorkflowState) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "reconcile resource: %v", err)
	}

	return &boundflowv1.ReconcileResourceResponse{}, nil
}

func (h *ResourceLifecycleHandler) DeleteResource(ctx context.Context, req *boundflowv1.DeleteResourceRequest) (*boundflowv1.DeleteResourceResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	if err := h.svc.DeleteResource(ctx, req.CorrelationId, req.ResourceInstanceId); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "resource instance not found")
		}
		return nil, status.Errorf(codes.Internal, "delete resource: %v", err)
	}

	return &boundflowv1.DeleteResourceResponse{}, nil
}

func (h *ResourceLifecycleHandler) GetResourceState(ctx context.Context, req *boundflowv1.GetResourceStateRequest) (*boundflowv1.GetResourceStateResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	instance, err := h.svc.GetResourceState(ctx, req.ResourceInstanceId)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "resource instance not found")
		}
		return nil, status.Errorf(codes.Internal, "get resource state: %v", err)
	}
	return &boundflowv1.GetResourceStateResponse{
		ResourceInstance: convert.ResourceInstanceToProto(instance),
	}, nil
}

func (h *ResourceLifecycleHandler) SetAgentRuntimePolicy(ctx context.Context, req *boundflowv1.SetAgentRuntimePolicyRequest) (*boundflowv1.SetAgentRuntimePolicyResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}
	if req.AgentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "agent_name is required")
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	policy := map[string]any{}
	if req.RuntimePolicy != nil {
		policy = req.RuntimePolicy.AsMap()
	}
	if err := h.svc.SetAgentRuntimePolicy(ctx, req.ResourceInstanceId, req.AgentName, policy); err != nil {
		return nil, status.Errorf(codes.Internal, "set agent runtime policy: %v", err)
	}
	return &boundflowv1.SetAgentRuntimePolicyResponse{}, nil
}

func (h *ResourceLifecycleHandler) SetAgentLifecyclePolicy(ctx context.Context, req *boundflowv1.SetAgentLifecyclePolicyRequest) (*boundflowv1.SetAgentLifecyclePolicyResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}
	if req.AgentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "agent_name is required")
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	policy := map[string]any{}
	if req.LifecyclePolicy != nil {
		policy = req.LifecyclePolicy.AsMap()
	}
	if err := h.svc.SetAgentLifecyclePolicy(ctx, req.ResourceInstanceId, req.AgentName, policy); err != nil {
		return nil, status.Errorf(codes.Internal, "set agent lifecycle policy: %v", err)
	}
	return &boundflowv1.SetAgentLifecyclePolicyResponse{}, nil
}

func (h *ResourceLifecycleHandler) DeleteAgent(ctx context.Context, req *boundflowv1.DeleteAgentRequest) (*boundflowv1.DeleteAgentResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}
	if req.AgentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "agent_name is required")
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	if err := h.svc.DeleteAgent(ctx, req.ResourceInstanceId, req.AgentName); err != nil {
		return nil, status.Errorf(codes.Internal, "delete agent: %v", err)
	}
	return &boundflowv1.DeleteAgentResponse{}, nil
}

func (h *ResourceLifecycleHandler) SetWorkflowLifecyclePolicy(ctx context.Context, req *boundflowv1.SetWorkflowLifecyclePolicyRequest) (*boundflowv1.SetWorkflowLifecyclePolicyResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}
	for i, rule := range req.GetLifecyclePolicy().GetRules() {
		if rule.Action == nil {
			return nil, status.Errorf(codes.InvalidArgument, "rule %d: action is required", i)
		}
		if rule.Action.Type != boundflowv1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_SET_VERSION && rule.Window <= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "rule %d: window must be > 0 for %s actions", i, rule.Action.Type)
		}
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	policy := convert.WorkflowLifecyclePolicyFromProto(req.LifecyclePolicy)
	if err := h.svc.SetWorkflowLifecyclePolicy(ctx, req.ResourceInstanceId, policy); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "resource instance not found")
		}
		return nil, status.Errorf(codes.Internal, "set workflow lifecycle policy: %v", err)
	}
	return &boundflowv1.SetWorkflowLifecyclePolicyResponse{}, nil
}

func (h *ResourceLifecycleHandler) ActivateWorkflow(ctx context.Context, req *boundflowv1.ActivateWorkflowRequest) (*boundflowv1.ActivateWorkflowResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	if err := h.svc.ActivateWorkflow(ctx, req.ResourceInstanceId); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "resource instance not found")
		}
		return nil, status.Errorf(codes.Internal, "activate workflow: %v", err)
	}
	return &boundflowv1.ActivateWorkflowResponse{}, nil
}

func (h *ResourceLifecycleHandler) ApproveWorkflow(ctx context.Context, req *boundflowv1.ApproveWorkflowRequest) (*boundflowv1.ApproveWorkflowResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}
	if req.ApprovalId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "approval_id is required")
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	if err := h.svc.ApproveWorkflow(ctx, req.ResourceInstanceId, req.ApprovalId); err != nil {
		if errors.Is(err, service.ErrInvalidWorkflowState) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "approve workflow: %v", err)
	}
	return &boundflowv1.ApproveWorkflowResponse{}, nil
}

func (h *ResourceLifecycleHandler) RejectWorkflow(ctx context.Context, req *boundflowv1.RejectWorkflowRequest) (*boundflowv1.RejectWorkflowResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}
	if req.ApprovalId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "approval_id is required")
	}

	if err := h.checkResourceOwner(ctx, req.ResourceInstanceId); err != nil {
		return nil, err
	}

	if err := h.svc.RejectWorkflow(ctx, req.ResourceInstanceId, req.ApprovalId); err != nil {
		if errors.Is(err, service.ErrInvalidWorkflowState) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "reject workflow: %v", err)
	}
	return &boundflowv1.RejectWorkflowResponse{}, nil
}
