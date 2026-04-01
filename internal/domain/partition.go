package domain

import "time"

type SchedulerPartition struct {
	ID                    string
	ResourceInstanceCount int
	Owner                 *string
	LeaseUntil            *time.Time
}
