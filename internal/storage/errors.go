package storage

import "errors"

var (
	ErrAlreadyExists              = errors.New("already exists")
	ErrNotFound                   = errors.New("not found")
	ErrInvalidLifecycleState      = errors.New("invalid lifecycle state")
	ErrDeletionAlreadyRequested   = errors.New("deletion already requested")
	ErrSuspensionAlreadyRequested = errors.New("workflow is already suspended, deleted, or a suspension is in flight")
	ErrTenantHasWorkflows         = errors.New("tenant still has workflows that are not deleted")
	ErrTenantDeleted              = errors.New("tenant has been deleted")
)
