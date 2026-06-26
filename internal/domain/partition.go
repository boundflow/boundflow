package domain

import "time"

type SchedulerPartition struct {
	ID                    string
	WorkflowCount int
	Owner                 *string
	LeaseUntil            *time.Time
}
