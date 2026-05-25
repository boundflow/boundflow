package domain

import "time"

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
	Owner                  *string
	LeaseExpiresAt         *time.Time
	CreatedAt              time.Time
}
