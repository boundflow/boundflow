package domain

import "time"

type LifecycleState string

const (
	LifecycleStateCreating    LifecycleState = "creating"
	LifecycleStateActive      LifecycleState = "active"
	LifecycleStateReconciling LifecycleState = "reconciling"
	LifecycleStateAwaitingApproval LifecycleState = "awaiting_approval"
	LifecycleStateDeleting         LifecycleState = "deleting"
	LifecycleStateDeleted          LifecycleState = "deleted"
	LifecycleStateFailed           LifecycleState = "failed"
)

type WorkflowConfig struct {
	InvokeTimeoutSeconds int32
	RepeatEverySeconds   int32
	Triggerable          bool
}

type WorkflowState string

const (
	WorkflowStateActive   WorkflowState = "active"
	WorkflowStatePaused   WorkflowState = "paused"
	WorkflowStateCooldown WorkflowState = "cooldown"
	WorkflowStateDisabled WorkflowState = "disabled"
)

type ResourceInstance struct {
	ID                     string
	TenantID               string
	ResourceType           string
	WorkflowConfig         WorkflowConfig
	LifecycleState         LifecycleState
	WorkflowState          WorkflowState
	LifecyclePolicy        WorkflowLifecyclePolicy
	InvocationMetrics      []WorkflowInvocationSnapshot
	CooldownUntil          *time.Time
	LifecycleLastResolved  int64
	CurrentWorkflowVersion int
	SchedulerPartitionID   string
	TargetVersion          int64
	CurrentVersion         int64
	LastCompletedRequestAt *time.Time
	CreatedAt              time.Time
}
