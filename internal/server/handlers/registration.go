package handlers

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/auth"
	"github.com/convergeplane/convergeplane/internal/convert"
	"github.com/convergeplane/convergeplane/internal/service"
	"github.com/convergeplane/convergeplane/internal/storage"
)

type RegistrationHandler struct {
	convergeplanev1.UnimplementedRegistrationServiceServer
	svc *service.RegistrationService
}

func NewRegistrationHandler(svc *service.RegistrationService) *RegistrationHandler {
	return &RegistrationHandler{svc: svc}
}

// callerTenantGroup extracts the tenant group ID injected by the auth interceptor.
func callerTenantGroup(ctx context.Context) (string, error) {
	id, ok := auth.TenantGroupFromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing auth")
	}
	return id, nil
}

func (h *RegistrationHandler) GetTenantGroup(ctx context.Context, req *convergeplanev1.GetTenantGroupRequest) (*convergeplanev1.GetTenantGroupResponse, error) {
	if req.Id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "id is required")
	}

	callerGroup, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	if req.Id != callerGroup {
		return nil, status.Error(codes.NotFound, "tenant group not found")
	}

	group, err := h.svc.GetTenantGroup(ctx, req.Id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tenant group not found")
		}
		return nil, status.Errorf(codes.Internal, "get tenant group: %v", err)
	}

	return &convergeplanev1.GetTenantGroupResponse{
		TenantGroup: convert.TenantGroupToProto(group),
	}, nil
}

func (h *RegistrationHandler) DeleteTenantGroup(ctx context.Context, req *convergeplanev1.DeleteTenantGroupRequest) (*convergeplanev1.DeleteTenantGroupResponse, error) {
	if req.Id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "id is required")
	}

	callerGroup, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	if req.Id != callerGroup {
		return nil, status.Error(codes.NotFound, "tenant group not found")
	}

	if err := h.svc.DeleteTenantGroup(ctx, req.Id); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tenant group not found")
		}
		return nil, status.Errorf(codes.Internal, "delete tenant group: %v", err)
	}

	return &convergeplanev1.DeleteTenantGroupResponse{}, nil
}

func (h *RegistrationHandler) CreateTenant(ctx context.Context, req *convergeplanev1.CreateTenantRequest) (*convergeplanev1.CreateTenantResponse, error) {
	tenant, err := convert.TenantFromProto(req.GetTenant())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	if groupID, ok := auth.TenantGroupFromContext(ctx); ok {
		tenant.TenantGroupID = groupID
	}

	result, err := h.svc.CreateTenant(ctx, tenant)
	if err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "tenant already exists")
		}
		return nil, status.Errorf(codes.Internal, "create tenant: %v", err)
	}

	return &convergeplanev1.CreateTenantResponse{
		Tenant: convert.TenantToProto(result),
	}, nil
}

func (h *RegistrationHandler) GetTenant(ctx context.Context, req *convergeplanev1.GetTenantRequest) (*convergeplanev1.GetTenantResponse, error) {
	if req.Id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "id is required")
	}

	tenant, err := h.svc.GetTenant(ctx, req.Id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tenant not found")
		}
		return nil, status.Errorf(codes.Internal, "get tenant: %v", err)
	}

	callerGroup, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	if tenant.TenantGroupID != callerGroup {
		return nil, status.Error(codes.NotFound, "tenant not found")
	}

	return &convergeplanev1.GetTenantResponse{
		Tenant: convert.TenantToProto(tenant),
	}, nil
}

func (h *RegistrationHandler) DeleteTenant(ctx context.Context, req *convergeplanev1.DeleteTenantRequest) (*convergeplanev1.DeleteTenantResponse, error) {
	if req.Id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "id is required")
	}

	tenant, err := h.svc.GetTenant(ctx, req.Id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tenant not found")
		}
		return nil, status.Errorf(codes.Internal, "get tenant: %v", err)
	}

	callerGroup, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}
	if tenant.TenantGroupID != callerGroup {
		return nil, status.Error(codes.NotFound, "tenant not found")
	}

	if err := h.svc.DeleteTenant(ctx, req.Id); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tenant not found")
		}
		return nil, status.Errorf(codes.Internal, "delete tenant: %v", err)
	}

	return &convergeplanev1.DeleteTenantResponse{}, nil
}
