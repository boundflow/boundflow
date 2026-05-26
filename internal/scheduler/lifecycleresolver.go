package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

type LifecycleResolver struct {
	interval int
	log      *slog.Logger
	resolver storage.LifecycleResolverRepository
	resource storage.ResourceInstanceRepository
}

func NewLifecycleResolver(interval int, log *slog.Logger) *LifecycleResolver {
	return &LifecycleResolver{
		interval: interval,
		log:      log,
	}
}

func (r *LifecycleResolver) Run(ctx context.Context) error {
	r.log.Info("lifecycle resolver starting")

	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)

}

func (r *LifecycleResolver) ResolveLifecyclePolicy(ctx context.Context, resourceInstanceId string) error {

	workflow, err := r.resource.Get(ctx, resourceInstanceId)
	if err != nil {
		return fmt.Errorf("get resource instance %s: %w", resourceInstanceId, err)
	}

	return nil
}

func (r *LifecycleResolver) resolvePolicy(rollingMetrics *domain.WorkflowInvocationSnapshot, policy *domain.WorkflowLifecyclePolicy, versionMetrics *domain.WorkflowVersionMetrics) {

}
