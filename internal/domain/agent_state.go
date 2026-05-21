package domain

import "time"

// AgentState holds the server-managed policy and rolling invocation history for a named agent
// within a specific workflow (resource instance). Keyed by (ResourceInstanceID, AgentName).
type AgentState struct {
	ResourceInstanceID string
	AgentName          string
	RuntimePolicy      map[string]any
	LifecyclePolicy    map[string]any
	// InvocationMetrics is a circular buffer of per-run metric snapshots.
	// Each entry: {tokens_used, cost_usd, llm_calls, calls_per_tool, ran_at}
	InvocationMetrics []map[string]any
	UpdatedAt         time.Time
}
