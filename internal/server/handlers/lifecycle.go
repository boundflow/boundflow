package handlers

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

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
		if errors.Is(err, service.ErrInvalidRepeatInterval) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
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

	var initialContext map[string]any
	if req.InitialContext != nil {
		initialContext = req.InitialContext.AsMap()
	}

	requestID, err := h.svc.InvokeWorkflow(ctx, req.CorrelationId, req.WorkflowId, params, initialContext)
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
		if errors.Is(err, service.ErrQueueFull) {
			return nil, status.Errorf(codes.ResourceExhausted, "%v", err)
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

func (h *WorkflowServiceHandler) GetWorkflowLifecyclePolicy(ctx context.Context, req *boundflowv1.GetWorkflowLifecyclePolicyRequest) (*boundflowv1.GetWorkflowLifecyclePolicyResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}
	policy, err := h.svc.GetWorkflowLifecyclePolicy(ctx, req.WorkflowId)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workflow instance not found")
		}
		return nil, status.Errorf(codes.Internal, "get workflow lifecycle policy: %v", err)
	}
	return &boundflowv1.GetWorkflowLifecyclePolicyResponse{
		LifecyclePolicy: convert.WorkflowLifecyclePolicyToProto(policy),
	}, nil
}

func (h *WorkflowServiceHandler) GetAgentRuntimePolicy(ctx context.Context, req *boundflowv1.GetAgentRuntimePolicyRequest) (*boundflowv1.GetAgentRuntimePolicyResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if req.AgentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "agent_name is required")
	}
	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}
	policy, err := h.svc.GetAgentRuntimePolicy(ctx, req.WorkflowId, req.AgentName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get agent runtime policy: %v", err)
	}
	s, err := structpb.NewStruct(policy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode runtime policy: %v", err)
	}
	return &boundflowv1.GetAgentRuntimePolicyResponse{RuntimePolicy: s}, nil
}

func (h *WorkflowServiceHandler) GetAgentLifecyclePolicy(ctx context.Context, req *boundflowv1.GetAgentLifecyclePolicyRequest) (*boundflowv1.GetAgentLifecyclePolicyResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if req.AgentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "agent_name is required")
	}
	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}
	policy, err := h.svc.GetAgentLifecyclePolicy(ctx, req.WorkflowId, req.AgentName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get agent lifecycle policy: %v", err)
	}
	s, err := structpb.NewStruct(policy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode lifecycle policy: %v", err)
	}
	return &boundflowv1.GetAgentLifecyclePolicyResponse{LifecyclePolicy: s}, nil
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

func (h *WorkflowServiceHandler) ResolveInterruptedWorkflow(ctx context.Context, req *boundflowv1.ResolveInterruptedWorkflowRequest) (*boundflowv1.ResolveInterruptedWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if req.RequestId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "request_id is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	if err := h.svc.ResolveInterruptedWorkflow(ctx, req.WorkflowId, req.RequestId); err != nil {
		if errors.Is(err, service.ErrNotInterrupted) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "resolve interrupted workflow: %v", err)
	}
	return &boundflowv1.ResolveInterruptedWorkflowResponse{}, nil
}

func (h *WorkflowServiceHandler) ListWorkflowRuns(ctx context.Context, req *boundflowv1.ListWorkflowRunsRequest) (*boundflowv1.ListWorkflowRunsResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	runs, err := h.svc.ListWorkflowRuns(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list workflow runs: %v", err)
	}

	out := make([]*boundflowv1.Run, 0, len(runs))
	for _, r := range runs {
		out = append(out, convert.RunToProto(r))
	}
	return &boundflowv1.ListWorkflowRunsResponse{Runs: out}, nil
}

func (h *WorkflowServiceHandler) GetRequestInfo(ctx context.Context, req *boundflowv1.GetRequestInfoRequest) (*boundflowv1.GetRequestInfoResponse, error) {
	if req.RequestId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "request_id is required")
	}

	request, err := h.svc.GetRequestInfo(ctx, req.RequestId)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "request not found")
		}
		return nil, status.Errorf(codes.Internal, "get request info: %v", err)
	}

	// Scope by the request's workflow — a caller can only read their own runs.
	if err := h.checkWorkflowOwner(ctx, request.WorkflowID); err != nil {
		return nil, err
	}

	return &boundflowv1.GetRequestInfoResponse{Request: convert.RequestInfoToProto(request)}, nil
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
	events, err := h.svc.GetApprovalAudit(ctx, group, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get approval audit: %v", err)
	}
	out := make([]*boundflowv1.ApprovalAuditRecord, 0, len(events))
	for _, e := range events {
		rec, err := convert.ApprovalAuditRecord(e)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resolve approval audit: %v", err)
		}
		out = append(out, rec)
	}
	return &boundflowv1.GetApprovalAuditResponse{Records: out}, nil
}

func (h *WorkflowServiceHandler) GetApprovalAuditById(ctx context.Context, req *boundflowv1.GetApprovalAuditByIdRequest) (*boundflowv1.GetApprovalAuditByIdResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	event, err := h.svc.GetApprovalAuditByID(ctx, group, req.ApprovalId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get approval audit by id: %v", err)
	}
	if event == nil {
		return &boundflowv1.GetApprovalAuditByIdResponse{}, nil
	}
	rec, err := convert.ApprovalAuditRecord(*event)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolve approval audit: %v", err)
	}
	return &boundflowv1.GetApprovalAuditByIdResponse{Record: rec}, nil
}

func (h *WorkflowServiceHandler) SubmitInput(ctx context.Context, req *boundflowv1.SubmitInputRequest) (*boundflowv1.SubmitInputResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "workflow_id is required")
	}
	if req.InputId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "input_id is required")
	}

	if err := h.checkWorkflowOwner(ctx, req.WorkflowId); err != nil {
		return nil, err
	}

	var answer map[string]any
	if req.Answer != nil {
		answer = req.Answer.AsMap()
	}
	if err := h.svc.SubmitInput(ctx, req.WorkflowId, req.InputId, answer, req.Actor); err != nil {
		if errors.Is(err, service.ErrInvalidWorkflowState) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "submit input: %v", err)
	}
	return &boundflowv1.SubmitInputResponse{}, nil
}

func (h *WorkflowServiceHandler) GetInputAudit(ctx context.Context, req *boundflowv1.GetInputAuditRequest) (*boundflowv1.GetInputAuditResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	events, err := h.svc.GetInputAudit(ctx, group, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get input audit: %v", err)
	}
	out := make([]*boundflowv1.InputAuditRecord, 0, len(events))
	for _, e := range events {
		rec, err := convert.InputAuditRecord(e)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resolve input audit: %v", err)
		}
		out = append(out, rec)
	}
	return &boundflowv1.GetInputAuditResponse{Records: out}, nil
}

func (h *WorkflowServiceHandler) GetWorkflowPolicyAudit(ctx context.Context, req *boundflowv1.GetWorkflowPolicyAuditRequest) (*boundflowv1.GetWorkflowPolicyAuditResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	events, err := h.svc.GetWorkflowPolicyAudit(ctx, group, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get workflow policy audit: %v", err)
	}
	out := make([]*boundflowv1.WorkflowPolicyAuditRecord, 0, len(events))
	for _, e := range events {
		rec, err := convert.WorkflowPolicyAuditRecord(e)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resolve workflow policy audit: %v", err)
		}
		out = append(out, rec)
	}
	return &boundflowv1.GetWorkflowPolicyAuditResponse{Records: out}, nil
}

func (h *WorkflowServiceHandler) GetAgentPolicyAudit(ctx context.Context, req *boundflowv1.GetAgentPolicyAuditRequest) (*boundflowv1.GetAgentPolicyAuditResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	events, err := h.svc.GetAgentPolicyAudit(ctx, group, req.WorkflowId, req.AgentName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get agent policy audit: %v", err)
	}
	out := make([]*boundflowv1.AgentPolicyAuditRecord, 0, len(events))
	for _, e := range events {
		rec, err := convert.AgentPolicyAuditRecord(e)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resolve agent policy audit: %v", err)
		}
		out = append(out, rec)
	}
	return &boundflowv1.GetAgentPolicyAuditResponse{Records: out}, nil
}

func (h *WorkflowServiceHandler) GetAuditLog(ctx context.Context, req *boundflowv1.GetAuditLogRequest) (*boundflowv1.GetAuditLogResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	events, err := h.svc.GetAuditLog(ctx, group, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get audit log: %v", err)
	}
	out := make([]*boundflowv1.AuditEntry, 0, len(events))
	for _, e := range events {
		entry, err := convert.AuditEntry(e)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resolve audit entry: %v", err)
		}
		out = append(out, entry)
	}
	return &boundflowv1.GetAuditLogResponse{Entries: out}, nil
}
