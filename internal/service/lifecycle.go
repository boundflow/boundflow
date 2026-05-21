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

var ErrMissingJobPolicy = errors.New("operation_timeout_seconds must be set on the request, tenant policy, or tenant group policy")

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

// resolveJobPolicy builds the JobPolicy for a new job by merging (in order):
// the per-request override, the tenant's policy overrides, the tenant group's policies.
// Returns ErrMissingJobPolicy if required fields cannot be resolved.
func (s *LifecycleService) resolveJobPolicy(ctx context.Context, tenantID string, requestPolicy domain.JobPolicy) (domain.JobPolicy, error) {
	resolved := requestPolicy

	if resolved.OperationTimeoutSeconds == 0 {
		tenant, err := s.tenants.Get(ctx, tenantID)
		if err != nil {
			return domain.JobPolicy{}, fmt.Errorf("look up tenant: %w", err)
		}
		if tenant.PolicyOverrides != nil && tenant.PolicyOverrides.OperationTimeoutSeconds > 0 {
			resolved.OperationTimeoutSeconds = tenant.PolicyOverrides.OperationTimeoutSeconds
		} else {
			group, err := s.tenantGroups.Get(ctx, tenant.TenantGroupID)
			if err != nil {
				return domain.JobPolicy{}, fmt.Errorf("look up tenant group: %w", err)
			}
			resolved.OperationTimeoutSeconds = group.Policies.OperationTimeoutSeconds
		}
	}

	if resolved.OperationTimeoutSeconds == 0 {
		return domain.JobPolicy{}, ErrMissingJobPolicy
	}

	return resolved, nil
}

func (s *LifecycleService) CreateResource(ctx context.Context, correlationID, resourceType, tenantID string, initialState domain.ResourceState, requestPolicy domain.JobPolicy) (*domain.ResourceInstance, error) {
	s.log.Info("creating resource", "correlation_id", correlationID, "resource_type", resourceType, "tenant_id", tenantID)

	policy, err := s.resolveJobPolicy(ctx, tenantID, requestPolicy)
	if err != nil {
		return nil, err
	}

	id := uuid.New().String()
	resourceInstance := domain.ResourceInstance{
		ID:                   id,
		TenantID:             tenantID,
		ResourceType:         resourceType,
		CurrentConfigState:   nil,
		ConfigGoalState:      initialState,
		LifecycleState:       domain.LifecycleStateCreating,
		SchedulerPartitionID: partitionForID(id, s.numPartitions),
		TargetVersion:        1,
		CurrentVersion:       0,
	}

	if err := s.resourceInstances.Create(ctx, &resourceInstance); err != nil {
		s.log.Error("failed to create resource instance", "correlation_id", correlationID, "error", err)
		return nil, fmt.Errorf("create resource: %w", err)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	request := domain.CustomerRequest{
		ID:                      uuid.New().String(),
		ResourceInstanceID:      resourceInstance.ID,
		Status:                  domain.CustomerRequestStatusUnscheduled,
		RequestType:             domain.CustomerRequestTypeCreate,
		RequestInfo:             requestInfo,
		Version:                 resourceInstance.TargetVersion,
		GoalConfigSnapshot: initialState,
		JobPolicy:          policy,
	}

	if err := s.customerRequests.Create(ctx, &request); err != nil {
		s.log.Error("failed to create customer request", "correlation_id", correlationID, "resource_id", resourceInstance.ID, "error", err)
		return nil, fmt.Errorf("create customer request: %w", err)
	}

	s.log.Info("resource created, attempting immediate schedule", "correlation_id", correlationID, "resource_id", resourceInstance.ID, "request_id", request.ID)
	if err := s.scheduler.ScheduleRequest(ctx, request.ID); err != nil {
		s.log.Warn("immediate schedule failed, scheduler will retry", "request_id", request.ID, "error", err)
	}

	return &resourceInstance, nil
}

func (s *LifecycleService) ReconcileResource(ctx context.Context, correlationID, resourceInstanceID string, goalState domain.ResourceState, requestPolicy domain.JobPolicy) error {
	s.log.Info("reconciling resource", "correlation_id", correlationID, "resource_id", resourceInstanceID)

	instance, err := s.resourceInstances.Get(ctx, resourceInstanceID)
	if err != nil {
		return fmt.Errorf("get resource instance: %w", err)
	}
	policy, err := s.resolveJobPolicy(ctx, instance.TenantID, requestPolicy)
	if err != nil {
		return err
	}

	ver, err := s.resourceInstances.ReconcileGoalStateAndIncrementVersion(ctx, resourceInstanceID, goalState,
		domain.LifecycleStateDeleting, domain.LifecycleStateDeleted)
	if err != nil {
		s.log.Error("failed to update goal state", "correlation_id", correlationID, "resource_id", resourceInstanceID, "error", err)
		return fmt.Errorf("reconcile resource: %w", err)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	request := domain.CustomerRequest{
		ID:                      uuid.New().String(),
		ResourceInstanceID:      resourceInstanceID,
		Status:                  domain.CustomerRequestStatusUnscheduled,
		RequestType:             domain.CustomerRequestTypeReconcile,
		RequestInfo:             requestInfo,
		Version:                 ver,
		GoalConfigSnapshot: goalState,
		JobPolicy:          policy,
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

func (s *LifecycleService) DeleteResource(ctx context.Context, correlationID, resourceInstanceID string, requestPolicy domain.JobPolicy) error {
	s.log.Info("deleting resource", "correlation_id", correlationID, "resource_id", resourceInstanceID)

	instance, err := s.resourceInstances.Get(ctx, resourceInstanceID)
	if err != nil {
		return fmt.Errorf("get resource instance: %w", err)
	}
	policy, err := s.resolveJobPolicy(ctx, instance.TenantID, requestPolicy)
	if err != nil {
		return err
	}

	ver, err := s.resourceInstances.UpdateLifecycleStateAndIncrementVersion(ctx, resourceInstanceID,
		domain.LifecycleStateDeleting, domain.LifecycleStateDeleting, domain.LifecycleStateDeleted)
	if err != nil {
		s.log.Error("failed to transition resource to deleting", "correlation_id", correlationID, "resource_id", resourceInstanceID, "error", err)
		return fmt.Errorf("soft delete resource: %w", err)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	request := domain.CustomerRequest{
		ID:                      uuid.New().String(),
		ResourceInstanceID:      resourceInstanceID,
		Status:                  domain.CustomerRequestStatusUnscheduled,
		RequestType:             domain.CustomerRequestTypeDelete,
		RequestInfo:             requestInfo,
		Version:   ver,
		JobPolicy: policy,
	}

	if err := s.customerRequests.Create(ctx, &request); err != nil {
		s.log.Error("failed to create delete request", "correlation_id", correlationID, "resource_id", resourceInstanceID, "error", err)
		return fmt.Errorf("delete customer request: %w", err)
	}

	s.log.Info("delete request created, attempting immediate schedule", "correlation_id", correlationID, "resource_id", resourceInstanceID, "request_id", request.ID, "version", ver)
	if err := s.scheduler.ScheduleRequest(ctx, request.ID); err != nil {
		s.log.Warn("immediate schedule failed, scheduler will retry", "request_id", request.ID, "error", err)
	}

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

func partitionForID(id string, numPartitions int) string {
	h := fnv.New32a()
	h.Write([]byte(id))
	return strconv.Itoa(int(h.Sum32()) % numPartitions)
}
