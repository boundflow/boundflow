package storage

import "errors"

var (
	ErrAlreadyExists         = errors.New("already exists")
	ErrNotFound              = errors.New("not found")
	ErrInvalidLifecycleState = errors.New("invalid lifecycle state")
)
