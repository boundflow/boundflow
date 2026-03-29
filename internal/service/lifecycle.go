package service

import (
	"context"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

type LifecycleService struct {
	resourceInstances storage.ResourceInstanceRepository
	customerRequests  storage.CustomerRequestRepository
}

func NewLifecycleService(
	resourceInstances storage.ResourceInstanceRepository,
	customerRequests storage.CustomerRequestRepository,
) *LifecycleService {
	return &LifecycleService{
		resourceInstances: resourceInstances,
		customerRequests:  customerRequests,
	}
}

func (s *LifecycleService) CreateResource(ctx context.Context, correlationID, resourceType, tenantID string, initialState domain.ResourceState) (*domain.ResourceInstance, error) {
	// TODO: implement
	return nil, nil
}

func (s *LifecycleService) ReconcileResource(ctx context.Context, correlationID, resourceInstanceID string, goalState domain.ResourceState) (string, error) {
	// TODO: implement
	return "", nil
}

func (s *LifecycleService) DeleteResource(ctx context.Context, correlationID, resourceInstanceID string) (string, error) {
	// TODO: implement
	return "", nil
}

func (s *LifecycleService) GetResourceHealth(ctx context.Context, correlationID, resourceInstanceID string, expectedState domain.ResourceState) (bool, string, error) {
	// TODO: implement
	return false, "", nil
}
