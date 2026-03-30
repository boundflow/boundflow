package handlers

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/convert"
	"github.com/convergeplane/convergeplane/internal/service"
	"github.com/convergeplane/convergeplane/internal/storage"
)

type ResourceLifecycleHandler struct {
	convergeplanev1.UnimplementedResourceLifecycleServiceServer
	svc *service.LifecycleService
}

func NewResourceLifecycleHandler(svc *service.LifecycleService) *ResourceLifecycleHandler {
	return &ResourceLifecycleHandler{svc: svc}
}

func (h *ResourceLifecycleHandler) CreateResource(ctx context.Context, req *convergeplanev1.CreateResourceRequest) (*convergeplanev1.CreateResourceResponse, error) {
	if req.ResourceType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_type is required")
	}
	if req.TenantId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tenant_id is required")
	}

	initialState := convert.ResourceStateFromProto(req.InitialState)

	instance, err := h.svc.CreateResource(ctx, req.CorrelationId, req.ResourceType, req.TenantId, initialState)
	if err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "resource instance already exists")
		}
		return nil, status.Errorf(codes.Internal, "create resource: %v", err)
	}

	return &convergeplanev1.CreateResourceResponse{
		ResourceInstance: convert.ResourceInstanceToProto(instance),
	}, nil
}

func (h *ResourceLifecycleHandler) ReconcileResource(ctx context.Context, req *convergeplanev1.ReconcileResourceRequest) (*convergeplanev1.ReconcileResourceResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}

	goalState := convert.ResourceStateFromProto(req.GoalState)

	if err := h.svc.ReconcileResource(ctx, req.CorrelationId, req.ResourceInstanceId, goalState); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "resource instance not found")
		}
		return nil, status.Errorf(codes.Internal, "reconcile resource: %v", err)
	}

	return &convergeplanev1.ReconcileResourceResponse{}, nil
}

func (h *ResourceLifecycleHandler) DeleteResource(ctx context.Context, req *convergeplanev1.DeleteResourceRequest) (*convergeplanev1.DeleteResourceResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}

	if err := h.svc.DeleteResource(ctx, req.CorrelationId, req.ResourceInstanceId); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "resource instance not found")
		}
		return nil, status.Errorf(codes.Internal, "delete resource: %v", err)
	}

	return &convergeplanev1.DeleteResourceResponse{}, nil
}

func (h *ResourceLifecycleHandler) GetResourceHealth(ctx context.Context, req *convergeplanev1.GetResourceHealthRequest) (*convergeplanev1.GetResourceHealthResponse, error) {
	if req.ResourceInstanceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "resource_instance_id is required")
	}

	healthy, message, err := h.svc.GetResourceHealth(ctx, req.CorrelationId, req.ResourceInstanceId)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "resource instance not found")
		}
		return nil, status.Errorf(codes.Internal, "get resource health: %v", err)
	}

	return &convergeplanev1.GetResourceHealthResponse{
		Healthy: healthy,
		Message: message,
	}, nil
}
