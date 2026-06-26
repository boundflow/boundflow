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
	ApproveJob(ctx context.Context, resourceInstanceID string, approvalID string) (bool, error)
	RejectJob(ctx context.Context, resourceInstanceID string, approvalID string) (bool, error)
}

type LifecycleService struct {
	resourceInstances storage.ResourceInstanceRepository
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
	resourceInstances storage.ResourceInstanceRepository,
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
		resourceInstances: resourceInstances,
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
func (s *LifecycleService) ResolveModelPricing(ctx context.Context, resourceInstanceID string, requestInfo map[string]any) error {
	groupID, err := s.resourceInstances.TenantGroupIDForResource(ctx, resourceInstanceID)
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

func (s *LifecycleService) ResolveRuntimeParams(params domain.WorkflowRuntimeParams, instance *domain.ResourceInstance, userTriggered bool, requestInfo map[string]any) error {
	if !instance.WorkflowConfig.Triggerable && userTriggered {
		return fmt.Errorf("workflow is not triggerable")
	}

	// The workflow version to run is resolved dynamically at schedule time from the live
	// resource (CurrentWorkflowVersion); it is intentionally not snapshotted here. This is
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

func (s *LifecycleService) ResolveAgentRuntimeParams(ctx context.Context, resourceInstanceID string, requestInfo map[string]any) error {
	agents, err := s.agentStates.GetAllForResource(ctx, resourceInstanceID)
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

func (s *LifecycleService) CreateResource(ctx context.Context, correlationID, resourceType, tenantID string, cfg domain.WorkflowConfig, version int) (*domain.ResourceInstance, error) {
	s.log.Info("creating resource", "correlation_id", correlationID, "resource_type", resourceType, "tenant_id", tenantID)

	id := uuid.New().String()
	resourceInstance := domain.ResourceInstance{
		ID:                     id,
		TenantID:               tenantID,
		ResourceType:           resourceType,
		WorkflowConfig:         cfg,
		LifecycleState:         domain.LifecycleStateActive,
		WorkflowState:          domain.WorkflowStatePaused,
		LifecyclePolicy:        domain.WorkflowLifecyclePolicy{Rules: []domain.WorkflowLifecyclePolicyRule{}},
		CurrentWorkflowVersion: version,
		SchedulerPartitionID:   partitionForID(id, s.numPartitions),
		TargetVersion:          0,
		CurrentVersion:         0,
	}

	if err := s.resourceInstances.Create(ctx, &resourceInstance); err != nil {
		s.log.Error("failed to create resource instance", "correlation_id", correlationID, "error", err)
		return nil, fmt.Errorf("create resource: %w", err)
	}

	s.log.Info("resource created and paused", "correlation_id", correlationID, "resource_id", resourceInstance.ID)
	return &resourceInstance, nil
}

func (s *LifecycleService) ReconcileResource(ctx context.Context, correlationID, resourceInstanceID string, params domain.WorkflowRuntimeParams) error {
	s.log.Info("reconciling resource", "correlation_id", correlationID, "resource_id", resourceInstanceID)

	instance, err := s.resourceInstances.Get(ctx, resourceInstanceID)
	if err != nil {
		return fmt.Errorf("get resource instance: %w", err)
	}

	if instance.WorkflowState != domain.WorkflowStateActive {
		return fmt.Errorf("%w: workflow is in state %s", ErrInvalidWorkflowState, instance.WorkflowState)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	if err := s.ResolveRuntimeParams(params, instance, true, requestInfo); err != nil {
		return err
	}
	if err := s.ResolveAgentRuntimeParams(ctx, resourceInstanceID, requestInfo); err != nil {
		return err
	}
	if err := s.ResolveModelPricing(ctx, resourceInstanceID, requestInfo); err != nil {
		return err
	}

	request := domain.CustomerRequest{
		ID:                 uuid.New().String(),
		ResourceInstanceID: resourceInstanceID,
		Status:             domain.CustomerRequestStatusUnscheduled,
		RequestType:        domain.CustomerRequestTypeReconcile,
		RequestInfo:        requestInfo,
	}

	// Atomically allocates the version, flips to reconciling, and inserts the request.
	ver, err := s.customerRequests.CreateInvocationRequest(ctx, &request,
		[]domain.LifecycleState{domain.LifecycleStateDeleting, domain.LifecycleStateDeleted})
	if err != nil {
		s.log.Error("failed to create reconcile request", "correlation_id", correlationID, "resource_id", resourceInstanceID, "error", err)
		return fmt.Errorf("create invocation request: %w", err)
	}

	s.log.Info("reconcile request created, attempting immediate schedule", "correlation_id", correlationID, "resource_id", resourceInstanceID, "request_id", request.ID, "version", ver)
	if err := s.scheduler.ScheduleRequest(ctx, request.ID); err != nil {
		s.log.Warn("immediate schedule failed, scheduler will retry", "request_id", request.ID, "error", err)
	}

	return nil
}

func (s *LifecycleService) DeleteResource(ctx context.Context, correlationID, resourceInstanceID string) error {
	s.log.Info("deleting resource", "correlation_id", correlationID, "resource_id", resourceInstanceID)

	if _, err := s.resourceInstances.Get(ctx, resourceInstanceID); err != nil {
		return fmt.Errorf("get resource instance: %w", err)
	}

	if err := s.resourceInstances.MarkDeleted(ctx, resourceInstanceID); err != nil {
		s.log.Error("failed to delete resource", "correlation_id", correlationID, "resource_id", resourceInstanceID, "error", err)
		return fmt.Errorf("delete resource: %w", err)
	}

	s.log.Info("resource deleted", "correlation_id", correlationID, "resource_id", resourceInstanceID)
	return nil
}

func (s *LifecycleService) GetResourceState(ctx context.Context, resourceInstanceID string) (*domain.ResourceInstance, error) {
	s.log.Debug("getting resource state", "resource_id", resourceInstanceID)
	return s.resourceInstances.Get(ctx, resourceInstanceID)
}

func (s *LifecycleService) SetAgentRuntimePolicy(ctx context.Context, resourceInstanceID, agentName string, policy map[string]any) error {
	s.log.Info("setting agent runtime policy", "resource_id", resourceInstanceID, "agent", agentName)
	if err := s.agentStates.UpsertRuntimePolicy(ctx, resourceInstanceID, agentName, policy); err != nil {
		return fmt.Errorf("upsert agent runtime policy: %w", err)
	}
	return nil
}

func (s *LifecycleService) SetAgentLifecyclePolicy(ctx context.Context, resourceInstanceID, agentName string, policy map[string]any) error {
	s.log.Info("setting agent lifecycle policy", "resource_id", resourceInstanceID, "agent", agentName)
	if err := s.agentStates.UpsertLifecyclePolicy(ctx, resourceInstanceID, agentName, policy); err != nil {
		return fmt.Errorf("upsert agent lifecycle policy: %w", err)
	}
	return nil
}

func (s *LifecycleService) DeleteAgent(ctx context.Context, resourceInstanceID, agentName string) error {
	s.log.Info("deleting agent state", "resource_id", resourceInstanceID, "agent", agentName)
	if err := s.agentStates.Delete(ctx, resourceInstanceID, agentName); err != nil {
		return fmt.Errorf("delete agent state: %w", err)
	}
	return nil
}

func (s *LifecycleService) SetWorkflowLifecyclePolicy(ctx context.Context, resourceInstanceID string, policy domain.WorkflowLifecyclePolicy) error {
	s.log.Info("setting workflow lifecycle policy", "resource_id", resourceInstanceID)
	if err := s.resourceInstances.UpdateLifecyclePolicy(ctx, resourceInstanceID, policy); err != nil {
		return fmt.Errorf("set workflow lifecycle policy: %w", err)
	}
	return nil
}

func (s *LifecycleService) ActivateWorkflow(ctx context.Context, resourceInstanceID string) error {
	s.log.Info("activating workflow", "resource_id", resourceInstanceID)
	if err := s.resourceInstances.UpdateWorkflowState(ctx, resourceInstanceID, domain.WorkflowStateActive); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return fmt.Errorf("activate workflow: %w", err)
	}
	return nil
}

func (s *LifecycleService) ApproveWorkflow(ctx context.Context, resourceInstanceID string, approvalID string) error {
	s.log.Info("approving workflow", "resource_id", resourceInstanceID, "approval_id", approvalID)
	resolved, err := s.approvalResolver.ApproveJob(ctx, resourceInstanceID, approvalID)
	if err != nil {
		return fmt.Errorf("approve workflow: %w", err)
	}
	if !resolved {
		return fmt.Errorf("%w: approval ID did not match or workflow is not awaiting approval", ErrInvalidWorkflowState)
	}
	return nil
}

func (s *LifecycleService) RejectWorkflow(ctx context.Context, resourceInstanceID string, approvalID string) error {
	s.log.Info("rejecting workflow", "resource_id", resourceInstanceID, "approval_id", approvalID)
	resolved, err := s.approvalResolver.RejectJob(ctx, resourceInstanceID, approvalID)
	if err != nil {
		return fmt.Errorf("reject workflow: %w", err)
	}
	if !resolved {
		return fmt.Errorf("%w: approval ID did not match or workflow is not awaiting approval", ErrInvalidWorkflowState)
	}
	return nil
}

// TenantGroupIDForResource returns the tenant_group_id that owns a resource (single JOIN).
// Returns storage.ErrNotFound if the resource does not exist.
func (s *LifecycleService) TenantGroupIDForResource(ctx context.Context, resourceInstanceID string) (string, error) {
	return s.resourceInstances.TenantGroupIDForResource(ctx, resourceInstanceID)
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
