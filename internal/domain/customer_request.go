package domain

import "time"

type CustomerRequestStatus string

const (
	CustomerRequestStatusUnscheduled CustomerRequestStatus = "unscheduled"
	CustomerRequestStatusScheduled   CustomerRequestStatus = "scheduled"
	CustomerRequestStatusInProgress  CustomerRequestStatus = "in_progress"
	CustomerRequestStatusFailed      CustomerRequestStatus = "failed"
	CustomerRequestStatusCompleted   CustomerRequestStatus = "completed"
	CustomerRequestStatusSuperceded  CustomerRequestStatus = "superceded"
)

type CustomerRequestType string

const (
	CustomerRequestTypeCreate      CustomerRequestType = "create"
	CustomerRequestTypeReconcile   CustomerRequestType = "reconcile"
	CustomerRequestTypeDelete      CustomerRequestType = "delete"
	CusomterRequestTypeHealthCheck CustomerRequestType = "healthcheck"
)

type CustomerRequest struct {
	ID                  string
	ResourceInstanceID  string
	SupercededRequestID string
	Status              CustomerRequestStatus
	RequestType         CustomerRequestType
	RequestInfo            map[string]any
	CurrentConfigSnapshot  ResourceState
	GoalConfigSnapshot     ResourceState
	Version                int64
	CreatedAt           time.Time
}
