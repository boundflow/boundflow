package handlers

import (
	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
)

type WorkerHandler struct {
	boundflowv1.UnimplementedWorkerServiceServer
}

func NewWorkerHandler() *WorkerHandler {
	return &WorkerHandler{}
}
