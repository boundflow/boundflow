package storage

import (
	"context"
	"time"

	"github.com/convergeplane/convergeplane/internal/domain"
)

type TenantGroupRepository interface {
	Create(ctx context.Context, group *domain.TenantGroup) error
	Get(ctx context.Context, id string) (*domain.TenantGroup, error)
	Delete(ctx context.Context, id string) error
}

type TenantRepository interface {
	Create(ctx context.Context, tenant *domain.Tenant) error
	Get(ctx context.Context, id string) (*domain.Tenant, error)
	Delete(ctx context.Context, id string) error
}

type ResourceInstanceRepository interface {
	Create(ctx context.Context, instance *domain.ResourceInstance) error
	Get(ctx context.Context, id string) (*domain.ResourceInstance, error)
	UpdateLifecycleState(ctx context.Context, id string, state domain.LifecycleState) error
	// UpdateLifecycleStateAndIncrementVersion atomically sets the lifecycle state and bumps the version.
	// Optionally pass lifecycle states that should cause the call to fail with ErrInvalidLifecycleState.
	UpdateLifecycleStateAndIncrementVersion(ctx context.Context, id string, state domain.LifecycleState, invalidStates ...domain.LifecycleState) (newVersion int64, err error)
	UpdateConfigState(ctx context.Context, id string, currentState, goalState domain.ResourceState) error
	// UpdateGoalStateAndIncrementVersion atomically sets the goal state and bumps the version.
	// Optionally pass lifecycle states that should cause the call to fail with ErrInvalidLifecycleState.
	UpdateGoalStateAndIncrementVersion(ctx context.Context, id string, goalState domain.ResourceState, invalidStates ...domain.LifecycleState) (newVersion int64, err error)
	// IncrementVersion atomically increments the version and returns the new value.
	IncrementVersion(ctx context.Context, id string) (newVersion int64, err error)
	UpdateSchedulerPartition(ctx context.Context, id string, partitionID string) error
	UpdateLastCompletedRequestAt(ctx context.Context, id string, t time.Time) error
}

type CustomerRequestRepository interface {
	Create(ctx context.Context, req *domain.CustomerRequest) error
	Get(ctx context.Context, resourceInstanceID, id string) (*domain.CustomerRequest, error)
	UpdateStatus(ctx context.Context, resourceInstanceID, id string, status domain.CustomerRequestStatus) error
	UpdateSupercededBy(ctx context.Context, resourceInstanceID, id string, supercededRequestID string) error
}
