package storage

import (
	"context"
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
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
	UpdateWorkflowState(ctx context.Context, id string, state domain.WorkflowState) error
	MarkDeleted(ctx context.Context, id string) error
	UpdateLifecyclePolicy(ctx context.Context, id string, policy domain.WorkflowLifecyclePolicy) error
	// UpdateLifecycleStateAndIncrementVersion atomically sets the lifecycle state and bumps the target version.
	// Optionally pass lifecycle states that should cause the call to fail with ErrInvalidLifecycleState.
	UpdateLifecycleStateAndIncrementVersion(ctx context.Context, id string, state domain.LifecycleState, invalidStates ...domain.LifecycleState) (newTargetVersion int64, err error)
	// StartInvocationAndIncrementVersion atomically sets lifecycle_state to invoking and bumps the target version.
	// Optionally pass lifecycle states that should cause the call to fail with ErrInvalidLifecycleState.
	StartInvocationAndIncrementVersion(ctx context.Context, id string, invalidStates ...domain.LifecycleState) (newTargetVersion int64, err error)
	// IncrementTargetVersion atomically increments the target version and returns the new value.
	IncrementTargetVersion(ctx context.Context, id string) (newVersion int64, err error)
	// UpdateCurrentVersion sets the current version to match the completed target version.
	UpdateCurrentVersion(ctx context.Context, id string, version int64) error
	// ApplyCompletedJob updates lifecycle_state and current_version for a resource, but only if
	// the provided version is strictly greater than the stored current_version.
	// Returns false if the version check failed (a newer completion already applied).
	ApplyCompletedJob(ctx context.Context, id string, lifecycleState domain.LifecycleState, version int64) (bool, error)
	UpdateSchedulerPartition(ctx context.Context, id string, partitionID string) error
	UpdateLastCompletedRequestAt(ctx context.Context, id string, t time.Time) error
	// TenantGroupIDForResource returns the tenant_group_id for a resource via a single JOIN.
	// Used for ownership checks. Returns ErrNotFound if the resource does not exist.
	TenantGroupIDForResource(ctx context.Context, resourceInstanceID string) (string, error)
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
	// GetDuePeriodicResources returns the active periodic resources in the partition whose next
	// invocation is due (repeat_every_seconds elapsed since last completion / creation).
	GetDuePeriodicResources(ctx context.Context, partitionID string) ([]*domain.ResourceInstance, error)
	// UpsertJobAndSchedule writes or overwrites the job for a resource only if the incoming
	// request has a strictly higher version than what's currently in the jobs table.
	// If the write happens it also atomically marks the customer request as scheduled.
	// Returns the resource instance ID and version written, and written=true, if the job was
	// written. Returns written=false if the existing job had an equal or higher version.
	// contextJSON is the fully-assembled job context (built in the scheduler layer).
	// currentAtomicOperation is the entry-point step name (also computed in the scheduler layer).
	// timeoutSeconds and workflowVersion are read from request info and passed directly to the jobs table.
	// expectedCurrentVersion guards the write: it only proceeds if the resource's current_version
	// still equals it (the run the caller validated against), else written=false.
	UpsertJobAndSchedule(ctx context.Context, requestID string, contextJSON string, currentAtomicOperation string, timeoutSeconds int, workflowVersion int, expectedCurrentVersion int64) (resourceInstanceID string, version int64, written bool, err error)
	// MarkResourceAwaitingApproval sets lifecycle_state = awaiting_approval for the given resource,
	// guarded so it only fires if the job still shows awaiting_approval status at update time.
	MarkResourceAwaitingApproval(ctx context.Context, resourceInstanceID string) error
	// SyncAwaitingApprovalStates sets lifecycle_state = awaiting_approval for all resource instances
	// in the partition that have a job in awaiting_approval status, guarded so it only fires if the
	// job still shows awaiting_approval at update time. Returns the synced resource instance IDs.
	SyncAwaitingApprovalStates(ctx context.Context, partitionID string) ([]string, error)
	// SupercedeOlderRequests marks all unscheduled or scheduled requests for the given resource
	// whose version is strictly less than version as superceded.
	SupercedeOlderRequests(ctx context.Context, resourceInstanceID string, version int64) error
	// DeleteTerminalJob deletes the job for the given resource only if the request ID matches
	// and the status is a terminal state (completed or failed).
	// Returns false if no matching job was deleted.
	DeleteTerminalJob(ctx context.Context, resourceInstanceID string, requestID string) (bool, error)
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
	// Returns false if the ownership check failed (job taken by another worker or released).
	UpdateJobStatus(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus) (bool, error)
	// UpdateJobStatusWithMetrics is UpdateJobStatus plus an atomic write of the accumulated
	// per-agent and workflow-level metrics. Used when finalizing a job.
	UpdateJobStatusWithMetrics(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus, agentMetrics map[string]*convergeplanev1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error)
	// UpdateJob updates status, current_atomic_operation, timeout_seconds, and context only if ownerID is the current owner.
	// Returns false if the ownership check failed (job taken by another worker or released).
	UpdateJob(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus, currentAtomicOperation string, operationTimeoutSeconds int, jobContext map[string]any) (bool, error)
	// UpdateJobWithMetrics is UpdateJob plus an atomic write of the accumulated per-agent
	// and workflow-level metrics. Used when advancing to the next operation.
	UpdateJobWithMetrics(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus, currentAtomicOperation string, operationTimeoutSeconds int, jobContext map[string]any, agentMetrics map[string]*convergeplanev1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error)
	// GetJobMetrics returns the accumulated per-agent and workflow-level metrics stored on the
	// job for the given resource and request. Returns zero values if no such job exists.
	GetJobMetrics(ctx context.Context, resourceInstanceID string, requestID string) (map[string]*convergeplanev1.AgentInvocationMetrics, domain.WorkflowJobMetrics, error)
	// ParkForApproval transitions a job to awaiting_approval, storing the approval ID,
	// timeout, and job metadata. Only succeeds if ownerID holds the job.
	// Returns false if ownership check fails.
	ParkForApproval(ctx context.Context, resourceInstanceID string, ownerID string, approvalID string, timeoutAt time.Time, metadata domain.JobMetadata, agentMetrics map[string]*convergeplanev1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error)
	// ResolveApproval transitions a job from awaiting_approval to the given status (approved/rejected),
	// guarded by approvalID match. Returns false if the ID doesn't match or the job isn't awaiting approval.
	ResolveApproval(ctx context.Context, resourceInstanceID string, approvalID string, status domain.JobStatus) (bool, error)
	// ReleaseJob clears the owner and lease on a job, only if currently owned by ownerID.
	ReleaseJob(ctx context.Context, resourceInstanceID string, ownerID string) error
}

type AgentStateRepository interface {
	// UpsertRuntimePolicy sets the runtime policy for an agent, creating the row if needed.
	UpsertRuntimePolicy(ctx context.Context, resourceInstanceID, agentName string, policy map[string]any) error
	// UpsertLifecyclePolicy sets the lifecycle policy for an agent, creating the row if needed.
	UpsertLifecyclePolicy(ctx context.Context, resourceInstanceID, agentName string, policy map[string]any) error
	// UpdateMetrics persists updated invocation metrics for an agent.
	UpdateMetrics(ctx context.Context, resourceInstanceID, agentName string, metrics []*convergeplanev1.AgentInvocationMetrics) error
	// GetAllForResource returns all agent states for a resource instance, keyed by agent name.
	GetAllForResource(ctx context.Context, resourceInstanceID string) (map[string]*domain.AgentState, error)
	// Delete removes the agent state row entirely.
	Delete(ctx context.Context, resourceInstanceID, agentName string) error
}

type LifecycleResolverRepository interface {
	// GetExpiredCooldownResources returns all resource instances in the given partition
	// whose workflow_state is 'cooldown' and whose cooldown_until has passed.
	GetExpiredCooldownResources(ctx context.Context, partitionID string) ([]*domain.ResourceInstance, error)
	// ExpireCooldowns atomically flips all expired-cooldown workflows in the partition back to
	// active and returns the IDs that were resumed.
	ExpireCooldowns(ctx context.Context, partitionID string) ([]string, error)
	// TryApplyPolicyResolution atomically updates lifecycle_last_resolved,
	// current_workflow_version, workflow_state, and cooldown_until only if the stored
	// lifecycle_last_resolved is less than resolved. Returns true if the update was applied.
	// cooldownUntil should be non-nil only when workflowState is WorkflowStateCooldown.
	TryApplyPolicyResolution(ctx context.Context, resourceInstanceID string, resolved int64, workflowVersion int, workflowState domain.WorkflowState, cooldownUntil *time.Time) (bool, error)
}

type MetricsRepository interface {
	// EmitMetrics atomically appends the rolling snapshot, upserts version-metric totals,
	// and upserts each agent's metrics history — only if metrics_emitted_at < emittedVersion.
	// Returns false if the gate fails (metrics already emitted for this run).
	EmitMetrics(ctx context.Context, resourceInstanceID string, emittedVersion int64, rollingMetrics []domain.WorkflowInvocationSnapshot, versionMetrics *domain.WorkflowVersionMetrics, agentMetrics map[string][]*convergeplanev1.AgentInvocationMetrics) (bool, error)
}

type VersionMetricsRepository interface {
	// GetCurrentVersionMetrics returns the metrics row with the highest epoch for the
	// given resource instance and version. Returns nil if no row exists yet.
	GetCurrentVersionMetrics(ctx context.Context, resourceInstanceID string, version int) (*domain.WorkflowVersionMetrics, error)
}

type ApiKeyRepository interface {
	// Create inserts a new API key. The caller is responsible for hashing the raw key before calling.
	Create(ctx context.Context, key *domain.ApiKey) error
	// GetByKeyHash looks up an active (non-revoked) API key by its hash.
	// Returns ErrNotFound if no active key matches.
	GetByKeyHash(ctx context.Context, keyHash string) (*domain.ApiKey, error)
	// Revoke sets revoked_at on the key with the given ID.
	Revoke(ctx context.Context, id string) error
}

type CustomerRequestRepository interface {
	Create(ctx context.Context, req *domain.CustomerRequest) error
	// CreateInvocationRequest atomically bumps the resource's target_version, flips lifecycle_state
	// to reconciling, and inserts req with the allocated version (one statement). Fails with
	// ErrInvalidLifecycleState if the resource is in one of invalidStates. Sets req.Version.
	CreateInvocationRequest(ctx context.Context, req *domain.CustomerRequest, invalidStates []domain.LifecycleState) (int64, error)
	// CreateDuePeriodicRequest is CreateInvocationRequest gated by the periodic guards (no
	// non-terminal request in flight, last terminal completion at least minGap ago). Returns
	// created=false (no error) when a guard rejects. One atomic statement.
	CreateDuePeriodicRequest(ctx context.Context, req *domain.CustomerRequest, minGap time.Duration, invalidStates []domain.LifecycleState) (int64, bool, error)
	Get(ctx context.Context, id string) (*domain.CustomerRequest, error)
	UpdateStatus(ctx context.Context, resourceInstanceID, id string, status domain.CustomerRequestStatus) error
	// CompleteRequest sets the request status to completed and returns the full updated request.
	CompleteRequest(ctx context.Context, id string) (*domain.CustomerRequest, error)
	// FailRequest sets the request status to failed and returns the full updated request.
	FailRequest(ctx context.Context, id string) (*domain.CustomerRequest, error)
}
