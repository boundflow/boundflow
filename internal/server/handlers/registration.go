package handlers

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/auth"
	"github.com/boundflow/boundflow/internal/convert"
	"github.com/boundflow/boundflow/internal/service"
	"github.com/boundflow/boundflow/internal/storage"
)

type RegistrationHandler struct {
	boundflowv1.UnimplementedRegistrationServiceServer
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

func (h *RegistrationHandler) SetModelPricing(ctx context.Context, req *boundflowv1.SetModelPricingRequest) (*boundflowv1.SetModelPricingResponse, error) {
	p, err := convert.ModelPricingFromProto(req.GetPricing())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}

	if err := h.svc.SetModelPricing(ctx, group, p); err != nil {
		return nil, status.Errorf(codes.Internal, "set model pricing: %v", err)
	}

	return &boundflowv1.SetModelPricingResponse{
		Pricing: convert.ModelPricingToProto(p),
	}, nil
}

func (h *RegistrationHandler) ListModelPricing(ctx context.Context, req *boundflowv1.ListModelPricingRequest) (*boundflowv1.ListModelPricingResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}

	pricing, err := h.svc.ListEffectivePricing(ctx, group)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list model pricing: %v", err)
	}

	out := make([]*boundflowv1.ModelPricing, 0, len(pricing))
	for _, p := range pricing {
		out = append(out, convert.ModelPricingToProto(p))
	}
	return &boundflowv1.ListModelPricingResponse{Pricing: out}, nil
}

func (h *RegistrationHandler) GetTenantGroup(ctx context.Context, req *boundflowv1.GetTenantGroupRequest) (*boundflowv1.GetTenantGroupResponse, error) {
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

	return &boundflowv1.GetTenantGroupResponse{
		TenantGroup: convert.TenantGroupToProto(group),
	}, nil
}

func (h *RegistrationHandler) DeleteTenantGroup(ctx context.Context, req *boundflowv1.DeleteTenantGroupRequest) (*boundflowv1.DeleteTenantGroupResponse, error) {
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

	return &boundflowv1.DeleteTenantGroupResponse{}, nil
}

func (h *RegistrationHandler) CreateTenant(ctx context.Context, req *boundflowv1.CreateTenantRequest) (*boundflowv1.CreateTenantResponse, error) {
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

	return &boundflowv1.CreateTenantResponse{
		Tenant: convert.TenantToProto(result),
	}, nil
}

func (h *RegistrationHandler) GetTenant(ctx context.Context, req *boundflowv1.GetTenantRequest) (*boundflowv1.GetTenantResponse, error) {
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

	return &boundflowv1.GetTenantResponse{
		Tenant: convert.TenantToProto(tenant),
	}, nil
}

func (h *RegistrationHandler) ListTenants(ctx context.Context, req *boundflowv1.ListTenantsRequest) (*boundflowv1.ListTenantsResponse, error) {
	group, err := callerTenantGroup(ctx)
	if err != nil {
		return nil, err
	}

	tenants, err := h.svc.ListTenants(ctx, group)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list tenants: %v", err)
	}

	out := make([]*boundflowv1.Tenant, 0, len(tenants))
	for _, t := range tenants {
		out = append(out, convert.TenantToProto(t))
	}
	return &boundflowv1.ListTenantsResponse{Tenants: out}, nil
}

func (h *RegistrationHandler) DeleteTenant(ctx context.Context, req *boundflowv1.DeleteTenantRequest) (*boundflowv1.DeleteTenantResponse, error) {
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

	return &boundflowv1.DeleteTenantResponse{}, nil
}
