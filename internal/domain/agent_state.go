package domain

import (
	"time"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
)

// AgentState holds the server-managed policy and rolling invocation history for a named agent
// within a specific workflow (workflow instance). Keyed by (WorkflowID, AgentName).
type AgentState struct {
	WorkflowID string
	AgentName          string
	RuntimePolicy      map[string]any
	LifecyclePolicy    map[string]any
	// InvocationMetrics is a rolling history of this agent's per-run metric snapshots,
	// ordered oldest-first (by RanAt).
	InvocationMetrics []*boundflowv1.AgentInvocationMetrics
	UpdatedAt         time.Time
}
