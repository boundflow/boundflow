package domain

import (
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
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
	ResourceInstanceID     string
	RequestID              string
	Version                int64
	CurrentAtomicOperation string
	Context                map[string]any
	Status                 JobStatus
	JobType                string
	ResourceType           string
	RuntimeParams          WorkflowRuntimeParams
	WorkflowVersion        int
	// AgentMetrics is the per-agent invocation metrics accumulated across this job's operations.
	AgentMetrics map[string]*convergeplanev1.AgentInvocationMetrics
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
// A nil branch means that path completes the job with no further operation.
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
