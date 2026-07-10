package domain

import "time"

type LifecycleState string

const (
	LifecycleStateCreating         LifecycleState = "creating"
	LifecycleStateActive           LifecycleState = "active"
	LifecycleStateScheduled        LifecycleState = "scheduled"
	LifecycleStateBlocked          LifecycleState = "blocked"
	LifecycleStateInvoking         LifecycleState = "invoking"
	LifecycleStateAwaitingApproval LifecycleState = "awaiting_approval"
	LifecycleStateAwaitingInput    LifecycleState = "awaiting_input"
	LifecycleStateDeleting         LifecycleState = "deleting"
	LifecycleStateDeleted          LifecycleState = "deleted"
	LifecycleStateInterrupted      LifecycleState = "interrupted"
)

// InvokeMode controls what happens when invokes pile up for a workflow (see the
// proto enum). Empty defaults to coalesce.
type InvokeMode string

const (
	InvokeModeCoalesce InvokeMode = "coalesce"
	InvokeModeQueue    InvokeMode = "queue"
)

// DefaultMaxQueueDepth bounds a queue-mode workflow's backlog when max_queue_depth
// is left unset (0), so queue mode is never truly unbounded.
const DefaultMaxQueueDepth = 1000

type WorkflowConfig struct {
	InvokeTimeoutSeconds int32
	RepeatEverySeconds   int32
	Triggerable          bool
	InvokeMode           InvokeMode
	MaxQueueDepth        int32
}

type WorkflowState string

const (
	WorkflowStateActive   WorkflowState = "active"
	WorkflowStatePaused   WorkflowState = "paused"
	WorkflowStateCooldown WorkflowState = "cooldown"
	WorkflowStateDisabled WorkflowState = "disabled"
)

type Workflow struct {
	ID                       string
	TenantID                 string
	WorkflowType             string
	WorkflowConfig           WorkflowConfig
	LifecycleState           LifecycleState
	WorkflowState            WorkflowState
	LifecyclePolicy          WorkflowLifecyclePolicy
	InvocationMetrics        []WorkflowInvocationSnapshot
	CooldownUntil            *time.Time
	LifecycleLastResolved    int64
	CurrentWorkflowVersion   int
	SchedulerPartitionID     string
	TargetVersion            int64
	CurrentVersion           int64
	LastCompletedRequestAt   *time.Time
	LastInterruptedRequestID string
	CreatedAt                time.Time
	// PendingApproval is the currently open approval gate, nil unless
	// LifecycleState == LifecycleStateAwaitingApproval.
	PendingApproval *PendingApproval
	// PendingInput is the currently open input gate, nil unless
	// LifecycleState == LifecycleStateAwaitingInput.
	PendingInput *PendingInput
}

// PendingApproval is the gate an external reader needs to discover and act on a
// parked approval (approve_workflow/reject_workflow take ApprovalID) without
// depending on the in-process worker's on_approval_requested hook.
type PendingApproval struct {
	ApprovalID    string
	Justification string
	Metadata      map[string]any
	OpenedAt      *time.Time
	TimeoutAt     *time.Time
}

// PendingInput is the gate an external reader needs to discover and act on a
// parked input request (submit_input takes InputID) without depending on the
// in-process worker's on_input_requested hook.
type PendingInput struct {
	InputID   string
	Prompt    string
	Metadata  map[string]any
	OpenedAt  *time.Time
	TimeoutAt *time.Time
}
