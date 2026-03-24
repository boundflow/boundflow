package handlers

import (
	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
)

type WorkerHandler struct {
	convergeplanev1.UnimplementedWorkerServiceServer
}

func NewWorkerHandler() *WorkerHandler {
	return &WorkerHandler{}
}
