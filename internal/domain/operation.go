package domain

import "time"

type OperationStatus int

const (
	OperationStatusUnspecified OperationStatus = iota
	OperationStatusContinue
	OperationStatusCompleted
	OperationStatusFailed
	OperationStatusRollback
)

type AtomicOperation struct {
	ID            string
	WorkflowID    string
	OperationType string
	Context       OperationContext
	NextOperation *AtomicOperation
	CreatedAt     time.Time
}

type OperationContext struct {
	Metadata map[string]string
	Payload  []byte
}

type AtomicOperationResult struct {
	Status        OperationStatus
	NextOperation *AtomicOperation
	Message       string
}
