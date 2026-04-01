package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

type LifecycleService struct {
	resourceInstances storage.ResourceInstanceRepository
	customerRequests  storage.CustomerRequestRepository
	scheduler         storage.SchedulerRepository
}

func NewLifecycleService(
	resourceInstances storage.ResourceInstanceRepository,
	customerRequests storage.CustomerRequestRepository,
	scheduler storage.SchedulerRepository,
) *LifecycleService {
	return &LifecycleService{
		resourceInstances: resourceInstances,
		customerRequests:  customerRequests,
		scheduler:         scheduler,
	}
}

func (s *LifecycleService) CreateResource(ctx context.Context, correlationID, resourceType, tenantID string, initialState domain.ResourceState) (*domain.ResourceInstance, error) {
	// Create resource in resource instance table

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
		return nil, fmt.Errorf("create resource: %w", err)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	// create customer request in customer requests table
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
		return nil, fmt.Errorf("create customer request: %w", err)
	}

	_, _, written, err := s.scheduler.UpsertJobAndSchedule(ctx, request.ID)

	if err == nil && written {
		s.scheduler.SupercedeOlderRequests(ctx, resourceInstance.ID, request.Version)
	}

	return &resourceInstance, nil
}

func (s *LifecycleService) ReconcileResource(ctx context.Context, correlationID, resourceInstanceID string, goalState domain.ResourceState) error {
	// Update goal state in resource instance table

	ver, err := s.resourceInstances.UpdateGoalStateAndIncrementVersion(ctx, resourceInstanceID, goalState,
		domain.LifecycleStateDeleting, domain.LifecycleStateDeleted)

	if err != nil {
		return fmt.Errorf("reconcile resource: %w", err)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	// create customer request in customer requests table
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
		return fmt.Errorf("reconcile customer request: %w", err)
	}

	_, _, written, err := s.scheduler.UpsertJobAndSchedule(ctx, request.ID)

	if err == nil && written {
		s.scheduler.SupercedeOlderRequests(ctx, resourceInstanceID, request.Version)
	}

	return nil
}

func (s *LifecycleService) DeleteResource(ctx context.Context, correlationID, resourceInstanceID string) error {
	// Put the resource in a "soft deleted state" (any operation post deletion should just fail out)

	ver, err := s.resourceInstances.UpdateLifecycleStateAndIncrementVersion(ctx, resourceInstanceID,
		domain.LifecycleStateDeleting, domain.LifecycleStateDeleted)

	if err != nil {
		return fmt.Errorf("Soft deleted resource: %w", err)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	// create customer request in customer requests table
	request := domain.CustomerRequest{
		ID:                 uuid.New().String(),
		ResourceInstanceID: resourceInstanceID,
		Status:             domain.CustomerRequestStatusUnscheduled,
		RequestType:        domain.CustomerRequestTypeDelete,
		RequestInfo:        requestInfo,
		Version:            ver,
	}

	if err := s.customerRequests.Create(ctx, &request); err != nil {
		return fmt.Errorf("delete customer request: %w", err)
	}

	_, _, written, err := s.scheduler.UpsertJobAndSchedule(ctx, request.ID)

	if err == nil && written {
		s.scheduler.SupercedeOlderRequests(ctx, resourceInstanceID, request.Version)
	}

	return nil
}

func (s *LifecycleService) GetResourceState(ctx context.Context, resourceInstanceID string) (*domain.ResourceInstance, error) {

	return s.resourceInstances.Get(ctx, resourceInstanceID)
}
