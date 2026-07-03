package domain

import "time"

type PolicySet struct {
	Concurrency             ConcurrencyPolicy
	Failure                 FailurePolicy
	MaintenanceWindow       MaintenanceWindowPolicy
	Upgrade                 UpgradePolicy
	OperationTimeoutSeconds int
}

type ConcurrencyPolicy struct {
	MaxConcurrentOperations int32
}

type FailurePolicy struct {
	EnableRollback bool
	Retry          RetryPolicy
}

type RetryPolicy struct {
	MaxRetries       int32
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	BackoffMultiplier float64
}

type MaintenanceWindowPolicy struct {
	Windows []MaintenanceWindow
}

type MaintenanceWindow struct {
	CronExpression string
	Duration       time.Duration
}

type UpgradePolicy struct {
	AutoPause bool
	BakeTime  time.Duration
}
