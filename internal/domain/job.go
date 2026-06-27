package domain

import (
	"time"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
)

type JobStatus string

const (
	JobStatusPending          JobStatus = "pending"
	JobStatusRunning          JobStatus = "running"
	JobStatusAwaitingNext     JobStatus = "awaiting_next"
	JobStatusAwaitingApproval JobStatus = "awaiting_approval"
	JobStatusApproved         JobStatus = "approved"
	JobStatusRejected         JobStatus = "rejected"
	JobStatusCompleted        JobStatus = "completed"
	JobStatusFailed           JobStatus = "failed"
)

type Job struct {
	WorkflowID     string
	RequestID              string
	Version                int64
	CurrentAtomicOperation string
	Context                map[string]any
	Status                 JobStatus
	JobType                string
	WorkflowType           string
	RuntimeParams          WorkflowRuntimeParams
	WorkflowVersion        int
	// AgentMetrics is the per-agent invocation metrics accumulated across this job's operations.
	AgentMetrics map[string]*boundflowv1.AgentInvocationMetrics
	// WorkflowMetrics is workflow-level (non-agent) metrics accumulated across this job's operations.
	WorkflowMetrics WorkflowJobMetrics
	Owner           *string
	LeaseExpiresAt  *time.Time
	CreatedAt       time.Time
	// Server-internal metadata for this job.
	JobMetadata JobMetadata
	// Approval gate — only populated when Status == JobStatusAwaitingApproval/Approved/Rejected.
	ApprovalID        *string
	ApprovalTimeoutAt *time.Time
}

// WorkflowJobMetrics holds workflow-level metrics accumulated across a job's operations.
// Serialized as JSONB in the jobs table (workflow_metrics column).
type WorkflowJobMetrics struct {
	Failures           int `json:"failures"`
	ApprovalRejections int `json:"approval_rejections"`
}

// OperationMetadata holds server-internal state for the current job operation.
// Serialized as JSONB in the jobs table.
type JobMetadata struct {
	ApprovalGate *ApprovalGateMetadata `json:"approval_gate,omitempty"`
}

// ApprovalGateMetadata stores the two possible continuations of an approval gate.
// A nil branch means that path completes the job with no further operation. The
// gate's audit data (opened_at, timeout_at) lives in jobs columns, not here.
type ApprovalGateMetadata struct {
	OnApprove *ApprovalBranch `json:"on_approve,omitempty"`
	OnReject  *ApprovalBranch `json:"on_reject,omitempty"`
}

// ApprovalBranch is the operation to dispatch when an approval gate resolves.
type ApprovalBranch struct {
	OperationName  string         `json:"name"`
	Context        map[string]any `json:"context"`
	TimeoutSeconds int            `json:"timeout_seconds"`
}

// ResolvedApproval carries the job bits an approval resolution needs to write its
// audit row (so the caller doesn't re-fetch the job after the status flip).
type ResolvedApproval struct {
	RequestID     string
	TenantGroupID string
	OpenedAt      *time.Time
}

// ExpiredApproval is a gate the scheduler resolved by timeout; carries everything
// needed to write its timed_out audit row.
type ExpiredApproval struct {
	WorkflowID    string
	RequestID     string
	TenantGroupID string
	ApprovalID    string
	OpenedAt      *time.Time
	TimedOutAt    time.Time // approval_timeout_at — the decided_at for a timeout
}
