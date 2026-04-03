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
	// UpdateLifecycleStateAndIncrementVersion atomically sets the lifecycle state and bumps the target version.
	// Optionally pass lifecycle states that should cause the call to fail with ErrInvalidLifecycleState.
	UpdateLifecycleStateAndIncrementVersion(ctx context.Context, id string, state domain.LifecycleState, invalidStates ...domain.LifecycleState) (newTargetVersion int64, err error)
	UpdateConfigState(ctx context.Context, id string, currentState, goalState domain.ResourceState) error
	// UpdateGoalStateAndIncrementVersion atomically sets the goal state and bumps the target version.
	// Optionally pass lifecycle states that should cause the call to fail with ErrInvalidLifecycleState.
	UpdateGoalStateAndIncrementVersion(ctx context.Context, id string, goalState domain.ResourceState, invalidStates ...domain.LifecycleState) (newTargetVersion int64, err error)
	// ReconcileGoalStateAndIncrementVersion atomically sets the lifecycle state to reconciling,
	// updates the goal config state, and bumps the target version.
	// Optionally pass lifecycle states that should cause the call to fail with ErrInvalidLifecycleState.
	ReconcileGoalStateAndIncrementVersion(ctx context.Context, id string, goalState domain.ResourceState, invalidStates ...domain.LifecycleState) (newTargetVersion int64, err error)
	// IncrementTargetVersion atomically increments the target version and returns the new value.
	IncrementTargetVersion(ctx context.Context, id string) (newVersion int64, err error)
	// UpdateCurrentVersion sets the current version to match the completed target version.
	UpdateCurrentVersion(ctx context.Context, id string, version int64) error
	// ApplyCompletedJob updates current_config_state, lifecycle_state, and current_version for
	// a resource, but only if the provided version is strictly greater than the stored current_version.
	// Returns false if the version check failed (a newer completion already applied).
	ApplyCompletedJob(ctx context.Context, id string, configState domain.ResourceState, lifecycleState domain.LifecycleState, version int64) (bool, error)
	UpdateSchedulerPartition(ctx context.Context, id string, partitionID string) error
	UpdateLastCompletedRequestAt(ctx context.Context, id string, t time.Time) error
}

type SchedulerPartitionRepository interface {
	// AcquireAvailable atomically claims any partition with no owner or an expired lease.
	// Returns nil partition (and nil error) if none are available.
	AcquireAvailable(ctx context.Context, ownerID string, leaseDuration time.Duration) (*domain.SchedulerPartition, error)
	// Renew extends the lease on a partition owned by ownerID or with an expired/unset lease.
	// Returns false if the partition could not be renewed (owned by someone else with a valid lease).
	Renew(ctx context.Context, partitionID string, ownerID string, leaseDuration time.Duration) (bool, error)
	// Release clears the owner on a partition, only if currently owned by ownerID.
	Release(ctx context.Context, partitionID string, ownerID string) error
}

// SchedulerRepository owns the scheduling queries run each tick per partition.
type SchedulerRepository interface {
	// GetTopUnscheduledRequests returns the IDs of the highest-version unscheduled customer
	// request for each resource instance belonging to the given partition.
	GetTopUnscheduledRequests(ctx context.Context, partitionID string) ([]string, error)
	// UpsertJobAndSchedule writes or overwrites the job for a resource only if the incoming
	// request has a strictly higher version than what's currently in the jobs table.
	// If the write happens it also atomically marks the customer request as scheduled.
	// Returns the resource instance ID and version written, and written=true, if the job was
	// written. Returns written=false if the existing job had an equal or higher version.
	UpsertJobAndSchedule(ctx context.Context, requestID string) (resourceInstanceID string, version int64, written bool, err error)
	// SupercedeOlderRequests marks all unscheduled or scheduled requests for the given resource
	// whose version is strictly less than version as superceded.
	SupercedeOlderRequests(ctx context.Context, resourceInstanceID string, version int64) error
	// ConsumeCompletedJob atomically deletes the job for the given resource if its status is
	// completed, returning the request ID it was targeting.
	// Returns found=false (no error) if no completed job exists for the resource.
	ConsumeCompletedJob(ctx context.Context, resourceInstanceID string) (requestID string, found bool, err error)
	// GetCompletedJobRequestIDs returns the request IDs of all completed jobs for resources
	// belonging to the given partition.
	GetCompletedJobRequestIDs(ctx context.Context, partitionID string) ([]string, error)
	// GetFailedJobRequestIDs returns the request IDs of all failed jobs for resources
	// belonging to the given partition.
	GetFailedJobRequestIDs(ctx context.Context, partitionID string) ([]string, error)
}

type JobRepository interface {
	// GetAvailableJob returns the resource instance ID of one job with status
	// pending or awaiting_next that has no owner or an expired lease.
	// Returns nil (no error) if none are available.
	GetAvailableJob(ctx context.Context) (resourceInstanceID *string, err error)
	// AcquireJob attempts to claim the job for ownerID, returning the full Job
	// if successful. Returns nil if the job no longer qualifies (taken by another worker).
	AcquireJob(ctx context.Context, resourceInstanceID string, ownerID string, leaseDuration time.Duration) (*domain.Job, error)
	// RenewJobLease extends the lease on a job owned by ownerID.
	// Returns false if the lease could not be renewed.
	RenewJobLease(ctx context.Context, resourceInstanceID string, ownerID string, leaseDuration time.Duration) (bool, error)
	// UpdateJobStatus updates the status of a job only if ownerID is the current owner.
	UpdateJobStatus(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus) error
	// UpdateJob updates status, context, and current_atomic_operation only if ownerID is the current owner.
	UpdateJob(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus, currentAtomicOperation string, jobContext map[string]any) error
	// ReleaseJob clears the owner and lease on a job, only if currently owned by ownerID.
	ReleaseJob(ctx context.Context, resourceInstanceID string, ownerID string) error
}

type CustomerRequestRepository interface {
	Create(ctx context.Context, req *domain.CustomerRequest) error
	Get(ctx context.Context, resourceInstanceID, id string) (*domain.CustomerRequest, error)
	UpdateStatus(ctx context.Context, resourceInstanceID, id string, status domain.CustomerRequestStatus) error
	// CompleteRequest sets the request status to completed and returns the full updated request.
	CompleteRequest(ctx context.Context, id string) (*domain.CustomerRequest, error)
	// FailRequest sets the request status to failed and returns the full updated request.
	FailRequest(ctx context.Context, id string) (*domain.CustomerRequest, error)
}
