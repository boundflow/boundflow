package domain

import (
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
)

// AgentState holds the server-managed policy and rolling invocation history for a named agent
// within a specific workflow (resource instance). Keyed by (ResourceInstanceID, AgentName).
type AgentState struct {
	ResourceInstanceID string
	AgentName          string
	RuntimePolicy      map[string]any
	LifecyclePolicy    map[string]any
	// InvocationMetrics is a rolling history of this agent's per-run metric snapshots,
	// ordered oldest-first (by RanAt).
	InvocationMetrics []*convergeplanev1.AgentInvocationMetrics
	UpdatedAt         time.Time
}
