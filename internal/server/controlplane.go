package server

import (
	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
)

type RegistrationHandler struct {
	convergeplanev1.UnimplementedRegistrationServiceServer
}

func NewRegistrationHandler() *RegistrationHandler {
	return &RegistrationHandler{}
}

type ResourceLifecycleHandler struct {
	convergeplanev1.UnimplementedResourceLifecycleServiceServer
}

func NewResourceLifecycleHandler() *ResourceLifecycleHandler {
	return &ResourceLifecycleHandler{}
}

type WorkerHandler struct {
	convergeplanev1.UnimplementedWorkerServiceServer
}

func NewWorkerHandler() *WorkerHandler {
	return &WorkerHandler{}
}
