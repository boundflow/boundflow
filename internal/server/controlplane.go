package server

import (
	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
)

type ControlPlaneHandler struct {
	convergeplanev1.UnimplementedControlPlaneServiceServer
}

func NewControlPlaneHandler() *ControlPlaneHandler {
	return &ControlPlaneHandler{}
}
