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

type CustomerRequest struct {
	ID                  string
	ResourceInstanceID  string
	SupercededRequestID string
	Status              CustomerRequestStatus
	RequestType         string
	RequestInfo         map[string]any
	CreatedAt           time.Time
}
