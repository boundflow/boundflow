package domain

import (
	"time"
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
	InvocationMetrics []AgentInvocationSnapshot
	UpdatedAt         time.Time
}

// AgentInvocationSnapshot is the stored, server-owned record of one agent run. It starts
// from the client-reported AgentInvocationMetrics (the wire message the worker sends) and
// adds RequestID -- server-known metadata stamped on at persistence time, not something the
// SDK reports itself. Deliberately a plain struct, not the proto type, so the wire contract
// and the storage representation can evolve independently (mirrors WorkflowInvocationSnapshot).
type AgentInvocationSnapshot struct {
	CostUsd            *float64       `json:"cost_usd,omitempty"`
	LlmCalls           *int           `json:"llm_calls,omitempty"`
	TokensUsed         *int           `json:"tokens_used,omitempty"`
	LatencySeconds     *float64       `json:"latency_seconds,omitempty"`
	Failures           *int           `json:"failures,omitempty"`
	ApprovalRejections *int           `json:"approval_rejections,omitempty"`
	ToolFailureCounts  map[string]int `json:"tool_failure_counts,omitempty"`
	CallsPerTool       map[string]int `json:"calls_per_tool,omitempty"`
	RanAt              int64          `json:"ran_at"`
	RequestID          string         `json:"request_id,omitempty"`
}
