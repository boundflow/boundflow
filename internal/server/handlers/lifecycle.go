package handlers

import (
	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
)

type ResourceLifecycleHandler struct {
	convergeplanev1.UnimplementedResourceLifecycleServiceServer
}

func NewResourceLifecycleHandler() *ResourceLifecycleHandler {
	return &ResourceLifecycleHandler{}
}
