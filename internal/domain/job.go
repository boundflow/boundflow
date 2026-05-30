package domain

import (
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
)

type JobStatus string

const (
	JobStatusPending      JobStatus = "pending"
	JobStatusRunning      JobStatus = "running"
	JobStatusAwaitingNext JobStatus = "awaiting_next"
	JobStatusCompleted    JobStatus = "completed"
	JobStatusFailed       JobStatus = "failed"
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
	AgentMetrics           map[string]*convergeplanev1.AgentInvocationMetrics
	Owner                  *string
	LeaseExpiresAt         *time.Time
	CreatedAt              time.Time
}
