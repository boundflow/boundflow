package storage

import "errors"

var (
	ErrAlreadyExists            = errors.New("already exists")
	ErrNotFound                 = errors.New("not found")
	ErrInvalidLifecycleState    = errors.New("invalid lifecycle state")
	ErrDeletionAlreadyRequested = errors.New("deletion already requested")
	ErrTenantHasWorkflows       = errors.New("tenant still has workflows that are not deleted")
	ErrTenantDeleted            = errors.New("tenant has been deleted")
)
