package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

// RequestScheduler is the scheduling capability the lifecycle service needs.
// Satisfied by *scheduler.Scheduler.
type RequestScheduler interface {
	ScheduleRequest(ctx context.Context, requestID string) error
}

type LifecycleService struct {
	resourceInstances storage.ResourceInstanceRepository
	customerRequests  storage.CustomerRequestRepository
	scheduler         RequestScheduler
	log               *slog.Logger
}

func NewLifecycleService(
	resourceInstances storage.ResourceInstanceRepository,
	customerRequests storage.CustomerRequestRepository,
	scheduler RequestScheduler,
	log *slog.Logger,
) *LifecycleService {
	return &LifecycleService{
		resourceInstances: resourceInstances,
		customerRequests:  customerRequests,
		scheduler:         scheduler,
		log:               log.With("component", "lifecycle_service"),
	}
}

func (s *LifecycleService) CreateResource(ctx context.Context, correlationID, resourceType, tenantID string, initialState domain.ResourceState) (*domain.ResourceInstance, error) {
	s.log.Info("creating resource", "correlation_id", correlationID, "resource_type", resourceType, "tenant_id", tenantID)

	resourceInstance := domain.ResourceInstance{
		ID:                   uuid.New().String(),
		TenantID:             tenantID,
		ResourceType:         resourceType,
		CurrentConfigState:   nil,
		ConfigGoalState:      initialState,
		LifecycleState:       domain.LifecycleStateCreating,
		SchedulerPartitionID: "", // TODO: ADD THIS EVEN FOR v1
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
		ID:                 uuid.New().String(),
		ResourceInstanceID: resourceInstance.ID,
		Status:             domain.CustomerRequestStatusUnscheduled,
		RequestType:        domain.CustomerRequestTypeCreate,
		RequestInfo:        requestInfo,
		Version:            resourceInstance.TargetVersion,
		GoalConfigSnapshot: initialState,
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

func (s *LifecycleService) ReconcileResource(ctx context.Context, correlationID, resourceInstanceID string, goalState domain.ResourceState) error {
	s.log.Info("reconciling resource", "correlation_id", correlationID, "resource_id", resourceInstanceID)

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
		ID:                 uuid.New().String(),
		ResourceInstanceID: resourceInstanceID,
		Status:             domain.CustomerRequestStatusUnscheduled,
		RequestType:        domain.CustomerRequestTypeReconcile,
		RequestInfo:        requestInfo,
		Version:            ver,
		GoalConfigSnapshot: goalState,
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
		ID:                 uuid.New().String(),
		ResourceInstanceID: resourceInstanceID,
		Status:             domain.CustomerRequestStatusUnscheduled,
		RequestType:        domain.CustomerRequestTypeDelete,
		RequestInfo:        requestInfo,
		Version:            ver,
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
