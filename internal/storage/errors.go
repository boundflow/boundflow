package storage

import "errors"

var (
	ErrAlreadyExists              = errors.New("already exists")
	ErrNotFound                   = errors.New("not found")
	ErrInvalidLifecycleState      = errors.New("invalid lifecycle state")
	ErrDeletionAlreadyRequested   = errors.New("deletion already requested")
	ErrSuspensionAlreadyRequested = errors.New("workflow cannot be suspended: it is disabled (deleted or interrupted), already held under a different suspension_id, or the hold named has finished draining and has nothing left to retarget")
	ErrTenantHasWorkflows         = errors.New("tenant still has workflows that are not deleted")
	ErrTenantDeleted              = errors.New("tenant has been deleted")
)
