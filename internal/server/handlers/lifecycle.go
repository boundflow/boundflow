package handlers

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/auth"
	"github.com/boundflow/boundflow/internal/convert"
	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/service"
	"github.com/boundflow/boundflow/internal/storage"
)

type WorkflowServiceHandler struct {
	boundflowv1.UnimplementedWorkflowServiceServer
	svc *service.LifecycleService
}

func NewWorkflowServiceHandler(svc *service.LifecycleService) *WorkflowServiceHandler {
	return &WorkflowServiceHandler{svc: svc}
}

// checkWorkflowOwner verifies the caller owns the given workflow instance.
// Returns NotFound (not PermissionDenied) on failure to avoid leaking existence.
func (h *WorkflowServiceHandler) checkWorkflowOwner(ctx context.Context, workflowID string) error {
	callerGroup, ok := auth.TenantGroupFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing auth")
	}
	ownerGroup, err := h.svc.TenantGroupIDForWorkflow(ctx, workflowID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return status.Error(codes.NotFound, "workflow instance not found")
		}
		return status.Errorf(codes.Internal, "check workflow owner: %v", err)
	}
	if ownerGroup != callerGroup {
		return status.Error(codes.NotFound, "workflow instance not found")
	}
	return nil
}

// checkTenantOwner verifies the caller owns the given tenant.
func (h *WorkflowServiceHandler) checkTenantOwner(ctx context.Context, tenantID string) error {
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

func (h *WorkflowServiceHandler) CreateWorkflow(ctx context.Context, req *boundflowv1.CreateWorkflowRequest) (*boundflowv1.CreateWorkflowResponse, error) {
	if req.WorkflowType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_type is required")
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

	instance, err := h.svc.CreateWorkflow(ctx, req.CorrelationId, req.WorkflowType, req.TenantId, cfg, version)
	if err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "workflow instance already exists")
		}
		return nil, status.Errorf(codes.Internal, "create workflow: %v", err)
	}

	return &boundflowv1.CreateWorkflowResponse{
		Workflow: convert.WorkflowToProto(instance),
	}, nil
}

func (h *WorkflowServiceHandler) InvokeWorkflow(ctx context.Context, req *boundflowv1.InvokeWorkflowRequest) (*boundflowv1.InvokeWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	var params domain.WorkflowRuntimeParams
	if req.RuntimeOverrides != nil {
		params.OperationTimeoutSeconds = int(req.RuntimeOverrides.OperationTimeoutSeconds)
	}

	requestID, err := h.svc.InvokeWorkflow(ctx, req.CorrelationId, req.WorkflowId, params)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workflow instance not found")
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
		return nil, status.Errorf(codes.Internal, "invoke workflow: %v", err)
	}

	return &boundflowv1.InvokeWorkflowResponse{RequestId: requestID}, nil
}

func (h *WorkflowServiceHandler) DeleteWorkflow(ctx context.Context, req *boundflowv1.DeleteWorkflowRequest) (*boundflowv1.DeleteWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	if err := h.svc.DeleteWorkflow(ctx, req.CorrelationId, req.WorkflowId); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workflow instance not found")
		}
		return nil, status.Errorf(codes.Internal, "delete workflow: %v", err)
	}

	return &boundflowv1.DeleteWorkflowResponse{}, nil
}

func (h *WorkflowServiceHandler) GetWorkflow(ctx context.Context, req *boundflowv1.GetWorkflowRequest) (*boundflowv1.GetWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	instance, err := h.svc.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workflow instance not found")
		}
		return nil, status.Errorf(codes.Internal, "get workflow state: %v", err)
	}
	return &boundflowv1.GetWorkflowResponse{
		Workflow: convert.WorkflowToProto(instance),
	}, nil
}

// ListWorkflows returns all workflows owned by the caller's tenant group. Scoping
// is implicit: the API-key auth interceptor injects the tenant group, so a caller
// can only ever see their own workflows.
func (h *WorkflowServiceHandler) ListWorkflows(ctx context.Context, _ *boundflowv1.ListWorkflowsRequest) (*boundflowv1.ListWorkflowsResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}

	instances, err := h.svc.ListWorkflows(ctx, group)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list workflows: %v", err)
	}

	out := make([]*boundflowv1.Workflow, 0, len(instances))
	for _, instance := range instances {
		out = append(out, convert.WorkflowToProto(instance))
	}
	return &boundflowv1.ListWorkflowsResponse{Workflows: out}, nil
}

func (h *WorkflowServiceHandler) SetAgentRuntimePolicy(ctx context.Context, req *boundflowv1.SetAgentRuntimePolicyRequest) (*boundflowv1.SetAgentRuntimePolicyResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if req.AgentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "agent_name is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	policy := map[string]any{}
	if req.RuntimePolicy != nil {
		policy = req.RuntimePolicy.AsMap()
	}
	if err := h.svc.SetAgentRuntimePolicy(ctx, req.WorkflowId, req.AgentName, policy); err != nil {
		return nil, status.Errorf(codes.Internal, "set agent runtime policy: %v", err)
	}
	return &boundflowv1.SetAgentRuntimePolicyResponse{}, nil
}

func (h *WorkflowServiceHandler) SetAgentLifecyclePolicy(ctx context.Context, req *boundflowv1.SetAgentLifecyclePolicyRequest) (*boundflowv1.SetAgentLifecyclePolicyResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if req.AgentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "agent_name is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	policy := map[string]any{}
	if req.LifecyclePolicy != nil {
		policy = req.LifecyclePolicy.AsMap()
	}
	if err := h.svc.SetAgentLifecyclePolicy(ctx, req.WorkflowId, req.AgentName, policy); err != nil {
		return nil, status.Errorf(codes.Internal, "set agent lifecycle policy: %v", err)
	}
	return &boundflowv1.SetAgentLifecyclePolicyResponse{}, nil
}

func (h *WorkflowServiceHandler) DeleteAgent(ctx context.Context, req *boundflowv1.DeleteAgentRequest) (*boundflowv1.DeleteAgentResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if req.AgentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "agent_name is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	if err := h.svc.DeleteAgent(ctx, req.WorkflowId, req.AgentName); err != nil {
		return nil, status.Errorf(codes.Internal, "delete agent: %v", err)
	}
	return &boundflowv1.DeleteAgentResponse{}, nil
}

func (h *WorkflowServiceHandler) SetWorkflowLifecyclePolicy(ctx context.Context, req *boundflowv1.SetWorkflowLifecyclePolicyRequest) (*boundflowv1.SetWorkflowLifecyclePolicyResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	for i, rule := range req.GetLifecyclePolicy().GetRules() {
		if rule.Action == nil {
			return nil, status.Errorf(codes.InvalidArgument, "rule %d: action is required", i)
		}
		if rule.Action.Type != boundflowv1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_SET_VERSION && rule.Window <= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "rule %d: window must be > 0 for %s actions", i, rule.Action.Type)
		}
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	policy := convert.WorkflowLifecyclePolicyFromProto(req.LifecyclePolicy)
	if err := h.svc.SetWorkflowLifecyclePolicy(ctx, req.WorkflowId, policy); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workflow instance not found")
		}
		return nil, status.Errorf(codes.Internal, "set workflow lifecycle policy: %v", err)
	}
	return &boundflowv1.SetWorkflowLifecyclePolicyResponse{}, nil
}

func (h *WorkflowServiceHandler) ActivateWorkflow(ctx context.Context, req *boundflowv1.ActivateWorkflowRequest) (*boundflowv1.ActivateWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	if err := h.svc.ActivateWorkflow(ctx, req.WorkflowId); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workflow instance not found")
		}
		return nil, status.Errorf(codes.Internal, "activate workflow: %v", err)
	}
	return &boundflowv1.ActivateWorkflowResponse{}, nil
}

func (h *WorkflowServiceHandler) ApproveWorkflow(ctx context.Context, req *boundflowv1.ApproveWorkflowRequest) (*boundflowv1.ApproveWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if req.ApprovalId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "approval_id is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	if err := h.svc.ApproveWorkflow(ctx, req.WorkflowId, req.ApprovalId, req.Actor); err != nil {
		if errors.Is(err, service.ErrInvalidWorkflowState) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "approve workflow: %v", err)
	}
	return &boundflowv1.ApproveWorkflowResponse{}, nil
}

func (h *WorkflowServiceHandler) RejectWorkflow(ctx context.Context, req *boundflowv1.RejectWorkflowRequest) (*boundflowv1.RejectWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if req.ApprovalId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "approval_id is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	if err := h.svc.RejectWorkflow(ctx, req.WorkflowId, req.ApprovalId, req.Actor); err != nil {
		if errors.Is(err, service.ErrInvalidWorkflowState) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "reject workflow: %v", err)
	}
	return &boundflowv1.RejectWorkflowResponse{}, nil
}

func (h *WorkflowServiceHandler) GetApprovalAudit(ctx context.Context, req *boundflowv1.GetApprovalAuditRequest) (*boundflowv1.GetApprovalAuditResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	events, err := h.svc.GetApprovalAudit(ctx, group, req.WorkflowId, req.ApprovalId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get approval audit: %v", err)
	}

	out := make([]*boundflowv1.ApprovalAuditRecord, 0, len(events))
	for _, e := range events {
		d, err := e.ApprovalDetails()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resolve approval audit: %v", err)
		}
		rec := &boundflowv1.ApprovalAuditRecord{
			ApprovalId: d.ApprovalID,
			WorkflowId: e.WorkflowID,
			RequestId:  e.RequestID,
			Decision:   approvalDecisionToProto(d.Decision),
			Actor:      e.Actor,
		}
		if d.OpenedAt != nil {
			rec.OpenedAt = timestamppb.New(*d.OpenedAt)
		}
		if d.DecidedAt != nil {
			rec.DecidedAt = timestamppb.New(*d.DecidedAt)
		}
		out = append(out, rec)
	}
	return &boundflowv1.GetApprovalAuditResponse{Records: out}, nil
}

func (h *WorkflowServiceHandler) GetPolicyAudit(ctx context.Context, req *boundflowv1.GetPolicyAuditRequest) (*boundflowv1.GetPolicyAuditResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	events, err := h.svc.GetPolicyAudit(ctx, group, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get policy audit: %v", err)
	}

	out := make([]*boundflowv1.PolicyAuditRecord, 0, len(events))
	for _, e := range events {
		d, err := e.PolicyDetails()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resolve policy audit: %v", err)
		}
		out = append(out, &boundflowv1.PolicyAuditRecord{
			WorkflowId:      e.WorkflowID,
			RequestId:       e.RequestID,
			Scope:           d.Scope,
			Rule:            convert.WorkflowRuleToProto(d.Rule),
			TriggerValue:    d.TriggerValue,
			PreviousVersion: int32(d.PreviousVersion),
			PreviousState:   string(d.PreviousState),
			Actor:           e.Actor,
			OccurredAt:      timestamppb.New(e.OccurredAt),
		})
	}
	return &boundflowv1.GetPolicyAuditResponse{Records: out}, nil
}

func approvalDecisionToProto(d domain.ApprovalDecision) boundflowv1.ApprovalDecision {
	switch d {
	case domain.ApprovalApproved:
		return boundflowv1.ApprovalDecision_APPROVAL_DECISION_APPROVED
	case domain.ApprovalRejected:
		return boundflowv1.ApprovalDecision_APPROVAL_DECISION_REJECTED
	case domain.ApprovalTimedOut:
		return boundflowv1.ApprovalDecision_APPROVAL_DECISION_TIMED_OUT
	default:
		return boundflowv1.ApprovalDecision_APPROVAL_DECISION_UNSPECIFIED
	}
}
