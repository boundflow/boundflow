package service

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"

	"github.com/google/uuid"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/pricing"
	"github.com/boundflow/boundflow/internal/storage"
)

var ErrMissingRuntimeParams = errors.New("operation_timeout_seconds must be set on the request, tenant policy, or tenant group policy")
var ErrInvalidWorkflowState = errors.New("workflow cannot be invoked in its current state")

// RequestScheduler is the scheduling capability the lifecycle service needs.
// Satisfied by *scheduler.Scheduler.
type RequestScheduler interface {
	ScheduleRequest(ctx context.Context, requestID string) error
}

// ApprovalResolver handles approve/reject for jobs awaiting approval.
// Satisfied by *scheduler.Scheduler.
type ApprovalResolver interface {
	ApproveJob(ctx context.Context, workflowID string, approvalID string) (bool, error)
	RejectJob(ctx context.Context, workflowID string, approvalID string) (bool, error)
}

type LifecycleService struct {
	workflows storage.WorkflowRepository
	customerRequests  storage.CustomerRequestRepository
	tenants           storage.TenantRepository
	tenantGroups      storage.TenantGroupRepository
	agentStates       storage.AgentStateRepository
	modelPricing      storage.ModelPricingRepository
	scheduler         RequestScheduler
	approvalResolver  ApprovalResolver
	numPartitions     int
	log               *slog.Logger
}

func NewLifecycleService(
	workflows storage.WorkflowRepository,
	customerRequests storage.CustomerRequestRepository,
	tenants storage.TenantRepository,
	tenantGroups storage.TenantGroupRepository,
	agentStates storage.AgentStateRepository,
	modelPricing storage.ModelPricingRepository,
	scheduler RequestScheduler,
	approvalResolver ApprovalResolver,
	numPartitions int,
	log *slog.Logger,
) *LifecycleService {
	return &LifecycleService{
		workflows: workflows,
		customerRequests:  customerRequests,
		tenants:           tenants,
		tenantGroups:      tenantGroups,
		agentStates:       agentStates,
		modelPricing:      modelPricing,
		scheduler:         scheduler,
		approvalResolver:  approvalResolver,
		numPartitions:     numPartitions,
		log:               log.With("component", "lifecycle_service"),
	}
}

// ResolveModelPricing snapshots the tenant group's effective per-model rates
// (built-in defaults merged with overrides) into the request context, so the
// worker can price token usage without a hardcoded table.
func (s *LifecycleService) ResolveModelPricing(ctx context.Context, workflowID string, requestInfo map[string]any) error {
	groupID, err := s.workflows.TenantGroupIDForWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("resolve tenant group for pricing: %w", err)
	}
	defaults, err := s.modelPricing.ListDefaults(ctx)
	if err != nil {
		return fmt.Errorf("list default pricing: %w", err)
	}
	overrides, err := s.modelPricing.ListForTenantGroup(ctx, groupID)
	if err != nil {
		return fmt.Errorf("list model pricing: %w", err)
	}
	effective := pricing.Effective(defaults, overrides)
	m := make(map[string]any, len(effective))
	for id, p := range effective {
		m[id] = map[string]any{"input_per_1m": p.InputPer1M, "output_per_1m": p.OutputPer1M}
	}
	requestInfo["modelPricing"] = m
	return nil
}

func (s *LifecycleService) ResolveRuntimeParams(params domain.WorkflowRuntimeParams, instance *domain.Workflow, userTriggered bool, requestInfo map[string]any) error {
	if !instance.WorkflowConfig.Triggerable && userTriggered {
		return fmt.Errorf("workflow is not triggerable")
	}

	// The workflow version to run is resolved dynamically at schedule time from the live
	// workflow (CurrentWorkflowVersion); it is intentionally not snapshotted here. This is
	// just a validation that a runnable version is configured.
	if instance.CurrentWorkflowVersion == 0 {
		return fmt.Errorf("no workflow version specified")
	}

	timeout := params.OperationTimeoutSeconds
	if timeout == 0 {
		timeout = int(instance.WorkflowConfig.InvokeTimeoutSeconds)
		if timeout == 0 {
			return fmt.Errorf("no invoke timeout specified")
		}
	}

	requestInfo["operationTimeoutSeconds"] = timeout
	return nil
}

func (s *LifecycleService) ResolveAgentRuntimeParams(ctx context.Context, workflowID string, requestInfo map[string]any) error {
	agents, err := s.agentStates.GetAllForWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get agent states: %w", err)
	}
	policies := make(map[string]any, len(agents))
	for name, a := range agents {
		policies[name] = a.RuntimePolicy
	}
	requestInfo["agentRuntimePolicies"] = policies
	return nil
}

func (s *LifecycleService) CreateWorkflow(ctx context.Context, correlationID, workflowType, tenantID string, cfg domain.WorkflowConfig, version int) (*domain.Workflow, error) {
	s.log.Info("creating workflow", "correlation_id", correlationID, "workflow_type", workflowType, "tenant_id", tenantID)

	id := uuid.New().String()
	workflow := domain.Workflow{
		ID:                     id,
		TenantID:               tenantID,
		WorkflowType:           workflowType,
		WorkflowConfig:         cfg,
		LifecycleState:         domain.LifecycleStateActive,
		WorkflowState:          domain.WorkflowStatePaused,
		LifecyclePolicy:        domain.WorkflowLifecyclePolicy{Rules: []domain.WorkflowLifecyclePolicyRule{}},
		CurrentWorkflowVersion: version,
		SchedulerPartitionID:   partitionForID(id, s.numPartitions),
		TargetVersion:          0,
		CurrentVersion:         0,
	}

	if err := s.workflows.Create(ctx, &workflow); err != nil {
		s.log.Error("failed to create workflow instance", "correlation_id", correlationID, "error", err)
		return nil, fmt.Errorf("create workflow: %w", err)
	}

	s.log.Info("workflow created and paused", "correlation_id", correlationID, "workflow_id", workflow.ID)
	return &workflow, nil
}

// InvokeWorkflow triggers a run and returns the created request's id — the run id
// the caller can use to correlate this invocation (e.g. with its trace).
func (s *LifecycleService) InvokeWorkflow(ctx context.Context, correlationID, workflowID string, params domain.WorkflowRuntimeParams) (string, error) {
	s.log.Info("invoking workflow", "correlation_id", correlationID, "workflow_id", workflowID)

	instance, err := s.workflows.Get(ctx, workflowID)
	if err != nil {
		return "", fmt.Errorf("get workflow instance: %w", err)
	}

	if instance.WorkflowState != domain.WorkflowStateActive {
		return "", fmt.Errorf("%w: workflow is in state %s", ErrInvalidWorkflowState, instance.WorkflowState)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	if err := s.ResolveRuntimeParams(params, instance, true, requestInfo); err != nil {
		return "", err
	}
	if err := s.ResolveAgentRuntimeParams(ctx, workflowID, requestInfo); err != nil {
		return "", err
	}
	if err := s.ResolveModelPricing(ctx, workflowID, requestInfo); err != nil {
		return "", err
	}

	request := domain.CustomerRequest{
		ID:                 uuid.New().String(),
		WorkflowID: workflowID,
		Status:             domain.CustomerRequestStatusUnscheduled,
		RequestType:        domain.CustomerRequestTypeInvoke,
		RequestInfo:        requestInfo,
	}

	// Atomically allocates the version, flips to invoking, and inserts the request.
	ver, err := s.customerRequests.CreateInvocationRequest(ctx, &request,
		[]domain.LifecycleState{domain.LifecycleStateDeleting, domain.LifecycleStateDeleted})
	if err != nil {
		s.log.Error("failed to create invoke request", "correlation_id", correlationID, "workflow_id", workflowID, "error", err)
		return "", fmt.Errorf("create invocation request: %w", err)
	}

	s.log.Info("invoke request created, attempting immediate schedule", "correlation_id", correlationID, "workflow_id", workflowID, "request_id", request.ID, "version", ver)
	if err := s.scheduler.ScheduleRequest(ctx, request.ID); err != nil {
		s.log.Warn("immediate schedule failed, scheduler will retry", "request_id", request.ID, "error", err)
	}

	return request.ID, nil
}

func (s *LifecycleService) DeleteWorkflow(ctx context.Context, correlationID, workflowID string) error {
	s.log.Info("deleting workflow", "correlation_id", correlationID, "workflow_id", workflowID)

	if _, err := s.workflows.Get(ctx, workflowID); err != nil {
		return fmt.Errorf("get workflow instance: %w", err)
	}

	if err := s.workflows.MarkDeleted(ctx, workflowID); err != nil {
		s.log.Error("failed to delete workflow", "correlation_id", correlationID, "workflow_id", workflowID, "error", err)
		return fmt.Errorf("delete workflow: %w", err)
	}

	s.log.Info("workflow deleted", "correlation_id", correlationID, "workflow_id", workflowID)
	return nil
}

func (s *LifecycleService) GetWorkflow(ctx context.Context, workflowID string) (*domain.Workflow, error) {
	s.log.Debug("getting workflow state", "workflow_id", workflowID)
	return s.workflows.Get(ctx, workflowID)
}

// ListWorkflows returns all workflows owned by the given tenant group, newest first.
func (s *LifecycleService) ListWorkflows(ctx context.Context, tenantGroupID string) ([]*domain.Workflow, error) {
	s.log.Debug("listing workflows", "tenant_group_id", tenantGroupID)
	return s.workflows.ListForTenantGroup(ctx, tenantGroupID)
}

func (s *LifecycleService) SetAgentRuntimePolicy(ctx context.Context, workflowID, agentName string, policy map[string]any) error {
	s.log.Info("setting agent runtime policy", "workflow_id", workflowID, "agent", agentName)
	if err := s.agentStates.UpsertRuntimePolicy(ctx, workflowID, agentName, policy); err != nil {
		return fmt.Errorf("upsert agent runtime policy: %w", err)
	}
	return nil
}

func (s *LifecycleService) SetAgentLifecyclePolicy(ctx context.Context, workflowID, agentName string, policy map[string]any) error {
	s.log.Info("setting agent lifecycle policy", "workflow_id", workflowID, "agent", agentName)
	if err := s.agentStates.UpsertLifecyclePolicy(ctx, workflowID, agentName, policy); err != nil {
		return fmt.Errorf("upsert agent lifecycle policy: %w", err)
	}
	return nil
}

func (s *LifecycleService) DeleteAgent(ctx context.Context, workflowID, agentName string) error {
	s.log.Info("deleting agent state", "workflow_id", workflowID, "agent", agentName)
	if err := s.agentStates.Delete(ctx, workflowID, agentName); err != nil {
		return fmt.Errorf("delete agent state: %w", err)
	}
	return nil
}

func (s *LifecycleService) SetWorkflowLifecyclePolicy(ctx context.Context, workflowID string, policy domain.WorkflowLifecyclePolicy) error {
	s.log.Info("setting workflow lifecycle policy", "workflow_id", workflowID)
	if err := s.workflows.UpdateLifecyclePolicy(ctx, workflowID, policy); err != nil {
		return fmt.Errorf("set workflow lifecycle policy: %w", err)
	}
	return nil
}

func (s *LifecycleService) ActivateWorkflow(ctx context.Context, workflowID string) error {
	s.log.Info("activating workflow", "workflow_id", workflowID)
	if err := s.workflows.UpdateWorkflowState(ctx, workflowID, domain.WorkflowStateActive); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return fmt.Errorf("activate workflow: %w", err)
	}
	return nil
}

func (s *LifecycleService) ApproveWorkflow(ctx context.Context, workflowID string, approvalID string) error {
	s.log.Info("approving workflow", "workflow_id", workflowID, "approval_id", approvalID)
	resolved, err := s.approvalResolver.ApproveJob(ctx, workflowID, approvalID)
	if err != nil {
		return fmt.Errorf("approve workflow: %w", err)
	}
	if !resolved {
		return fmt.Errorf("%w: approval ID did not match or workflow is not awaiting approval", ErrInvalidWorkflowState)
	}
	return nil
}

func (s *LifecycleService) RejectWorkflow(ctx context.Context, workflowID string, approvalID string) error {
	s.log.Info("rejecting workflow", "workflow_id", workflowID, "approval_id", approvalID)
	resolved, err := s.approvalResolver.RejectJob(ctx, workflowID, approvalID)
	if err != nil {
		return fmt.Errorf("reject workflow: %w", err)
	}
	if !resolved {
		return fmt.Errorf("%w: approval ID did not match or workflow is not awaiting approval", ErrInvalidWorkflowState)
	}
	return nil
}

// TenantGroupIDForWorkflow returns the tenant_group_id that owns a workflow (single JOIN).
// Returns storage.ErrNotFound if the workflow does not exist.
func (s *LifecycleService) TenantGroupIDForWorkflow(ctx context.Context, workflowID string) (string, error) {
	return s.workflows.TenantGroupIDForWorkflow(ctx, workflowID)
}

// TenantGroupIDForTenant returns the tenant_group_id for a tenant.
// Returns storage.ErrNotFound if the tenant does not exist.
func (s *LifecycleService) TenantGroupIDForTenant(ctx context.Context, tenantID string) (string, error) {
	tenant, err := s.tenants.Get(ctx, tenantID)
	if err != nil {
		return "", err
	}
	return tenant.TenantGroupID, nil
}

func partitionForID(id string, numPartitions int) string {
	h := fnv.New32a()
	h.Write([]byte(id))
	return strconv.Itoa(int(h.Sum32()) % numPartitions)
}
