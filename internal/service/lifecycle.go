package service

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"

	"github.com/google/uuid"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

var ErrMissingRuntimeParams = errors.New("operation_timeout_seconds must be set on the request, tenant policy, or tenant group policy")

// RequestScheduler is the scheduling capability the lifecycle service needs.
// Satisfied by *scheduler.Scheduler.
type RequestScheduler interface {
	ScheduleRequest(ctx context.Context, requestID string) error
}

type LifecycleService struct {
	resourceInstances storage.ResourceInstanceRepository
	customerRequests  storage.CustomerRequestRepository
	tenants           storage.TenantRepository
	tenantGroups      storage.TenantGroupRepository
	agentStates       storage.AgentStateRepository
	scheduler         RequestScheduler
	numPartitions     int
	log               *slog.Logger
}

func NewLifecycleService(
	resourceInstances storage.ResourceInstanceRepository,
	customerRequests storage.CustomerRequestRepository,
	tenants storage.TenantRepository,
	tenantGroups storage.TenantGroupRepository,
	agentStates storage.AgentStateRepository,
	scheduler RequestScheduler,
	numPartitions int,
	log *slog.Logger,
) *LifecycleService {
	return &LifecycleService{
		resourceInstances: resourceInstances,
		customerRequests:  customerRequests,
		tenants:           tenants,
		tenantGroups:      tenantGroups,
		agentStates:       agentStates,
		scheduler:         scheduler,
		numPartitions:     numPartitions,
		log:               log.With("component", "lifecycle_service"),
	}
}

func (s *LifecycleService) resolveRuntimeParams(params domain.WorkflowRuntimeParams, instance *domain.ResourceInstance, userTriggered bool, requestInfo map[string]any) error {
	if !instance.WorkflowConfig.Triggerable && userTriggered {
		return fmt.Errorf("workflow is not triggerable")
	}

	version := instance.CurrentWorkflowVersion
	if version == 0 {
		return fmt.Errorf("no workflow version specified")
	}

	timeout := params.OperationTimeoutSeconds
	if timeout == 0 {
		timeout = int(instance.WorkflowConfig.InvokeTimeoutSeconds)
		if timeout == 0 {
			return fmt.Errorf("no invoke timeout specified")
		}
	}

	requestInfo["initialVersion"] = version
	requestInfo["operationTimeoutSeconds"] = timeout
	return nil
}

func (s *LifecycleService) resolveAgentRuntimeParams(ctx context.Context, resourceInstanceID string, requestInfo map[string]any) error {
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
		CurrentWorkflowVersion: version,
		SchedulerPartitionID:   partitionForID(id, s.numPartitions),
		TargetVersion:          1,
		CurrentVersion:         1,
	}

	if err := s.resourceInstances.Create(ctx, &resourceInstance); err != nil {
		s.log.Error("failed to create resource instance", "correlation_id", correlationID, "error", err)
		return nil, fmt.Errorf("create resource: %w", err)
	}

	s.log.Info("resource created and active", "correlation_id", correlationID, "resource_id", resourceInstance.ID)
	return &resourceInstance, nil
}

func (s *LifecycleService) ReconcileResource(ctx context.Context, correlationID, resourceInstanceID string, params domain.WorkflowRuntimeParams) error {
	s.log.Info("reconciling resource", "correlation_id", correlationID, "resource_id", resourceInstanceID)

	instance, err := s.resourceInstances.Get(ctx, resourceInstanceID)
	if err != nil {
		return fmt.Errorf("get resource instance: %w", err)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	if err := s.resolveRuntimeParams(params, instance, true, requestInfo); err != nil {
		return err
	}
	if err := s.resolveAgentRuntimeParams(ctx, resourceInstanceID, requestInfo); err != nil {
		return err
	}

	ver, err := s.resourceInstances.StartInvocationAndIncrementVersion(ctx, resourceInstanceID,
		domain.LifecycleStateDeleting, domain.LifecycleStateDeleted)
	if err != nil {
		s.log.Error("failed to start invocation", "correlation_id", correlationID, "resource_id", resourceInstanceID, "error", err)
		return fmt.Errorf("start invocation: %w", err)
	}

	request := domain.CustomerRequest{
		ID:                 uuid.New().String(),
		ResourceInstanceID: resourceInstanceID,
		Status:             domain.CustomerRequestStatusUnscheduled,
		RequestType:        domain.CustomerRequestTypeReconcile,
		RequestInfo:        requestInfo,
		Version:            ver,
	}

	if err := s.customerRequests.Create(ctx, &request); err != nil {
		s.log.Error("failed to create reconcile request", "correlation_id", correlationID, "resource_id", resourceInstanceID, "error", err)
		return fmt.Errorf("reconcile customer request: %w", err)
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

	if err := s.resourceInstances.UpdateLifecycleState(ctx, resourceInstanceID, domain.LifecycleStateDeleted); err != nil {
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
	return fmt.Errorf("not implemented")
}

func (s *LifecycleService) ApproveWorkflow(ctx context.Context, resourceInstanceID string) error {
	s.log.Info("approving workflow", "resource_id", resourceInstanceID)
	return fmt.Errorf("not implemented")
}

func partitionForID(id string, numPartitions int) string {
	h := fnv.New32a()
	h.Write([]byte(id))
	return strconv.Itoa(int(h.Sum32()) % numPartitions)
}
