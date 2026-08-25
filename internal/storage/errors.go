package storage

import "errors"

var (
	ErrAlreadyExists              = errors.New("already exists")
	ErrNotFound                   = errors.New("not found")
	ErrInvalidLifecycleState      = errors.New("invalid lifecycle state")
	ErrDeletionAlreadyRequested   = errors.New("deletion already requested")
	ErrSuspensionAlreadyRequested = errors.New("workflow cannot be suspended: it is already suspended, has a suspension in flight, or is disabled (deleted or interrupted)")
	ErrTenantHasWorkflows         = errors.New("tenant still has workflows that are not deleted")
	ErrTenantDeleted              = errors.New("tenant has been deleted")
)
