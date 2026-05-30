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
	interval              int
	log                   *slog.Logger
	resolver              storage.LifecycleResolverRepository
	resource              storage.ResourceInstanceRepository
	versionMetrics        storage.VersionMetricsRepository
	partitionId           string
	lifecyclePolicyEngine *LifecyclePolicyEngine
}

func NewLifecycleResolver(interval int, log *slog.Logger, partitionId string, resolver storage.LifecycleResolverRepository, resource storage.ResourceInstanceRepository, versionMetrics storage.VersionMetricsRepository) *LifecycleResolver {
	return &LifecycleResolver{
		interval:              interval,
		log:                   log.With("component", "lifecycle_resolver"),
		partitionId:           partitionId,
		resolver:              resolver,
		resource:              resource,
		versionMetrics:        versionMetrics,
		lifecyclePolicyEngine: NewLifecyclePolicyEngine(log),
	}
}

// Q: Why does the resolver component only check for workflows in cooldown?
// A: Every other state is self-healing, for example: transitions from created, paused, and disabled
// are all control plane induced, and the control plane call will fail unless they happen
// From active, we know that on the next run there will be a necessary resolution via scheduler (see invariant below)
/*func (r *LifecycleResolver) Run(ctx context.Context) error {
	r.log.Info("lifecycle resolver starting")

	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			expiredCooldownWorkflows, err := r.resolver.GetExpiredCooldownResources(ctx, r.partitionId)
			if err != nil {
				r.log.Error("failed to get expired cooldown resources", "partition_id", r.partitionId, "error", err)
			} else {
				var wg sync.WaitGroup
				for _, wf := range expiredCooldownWorkflows {
					wg.Add(1)
					go func(wf *domain.ResourceInstance) {
						defer wg.Done()
						if err := r.resolveLifecyclePolicy(ctx, wf); err != nil {
							r.log.Error("failed to resolve lifecycle policy", "resource_id", wf.ID, "error", err)
						}
					}(wf)
				}
				wg.Wait()
			}
		case <-ctx.Done():
			r.log.Info("lifecycle resolver stopping", "partition_id", r.partitionId)
			return nil
		}
	}

}*/

// One very critical invariant: the resolver assumes that all prior runs except potentially the latest have been resolved
// This is enforced via the scheduler refusing to schedule an event until it has resolved succesfully for the current version
// This means a more correct name for this function is "Resolve Lifecycle Policy for latest run"
// Example: if version 2 wasn't resolved, version 3's resolution may not necessarily fix it (if metric wasnt emitted in latest run),
// but again this should be an impossible case due to invariant
func (r *LifecycleResolver) ResolveLifecyclePolicy(ctx context.Context, workflow *domain.ResourceInstance, versionMetrics *domain.WorkflowVersionMetrics) error {

	resourceInstanceId := workflow.ID

	if versionMetrics == nil {
		versionMetrics = &domain.WorkflowVersionMetrics{}
	}

	policy := workflow.LifecyclePolicy
	rollingMetrics := workflow.InvocationMetrics

	updated, goalState, err := r.lifecyclePolicyEngine.ResolvePolicy(&rollingMetrics, &policy, versionMetrics)

	if err != nil {
		return fmt.Errorf("Policy resolution failed with error %w", err)
	}

	if !updated {
		r.log.Debug("no lifecycle policy change", "resource_id", resourceInstanceId)
		return nil
	}

	version := workflow.CurrentWorkflowVersion
	state := workflow.WorkflowState
	cooldown := workflow.CooldownUntil

	if goalState.versionChange {
		version = goalState.version
	} else {
		state = goalState.state
		if state == domain.WorkflowStateCooldown {
			t := time.Now().Add(time.Duration(goalState.cooldown) * time.Second)
			cooldown = &t
		}
	}

	resolved, err := r.resolver.TryApplyPolicyResolution(ctx, resourceInstanceId, workflow.CurrentVersion, version, state, cooldown)

	if err != nil {
		return fmt.Errorf("Applying resolved polocy failed with error %w", err)
	}

	if !resolved {
		r.log.Debug("policy resolution skipped, already resolved at this version", "resource_id", resourceInstanceId, "current_version", workflow.CurrentVersion)
		return nil
	}

	r.log.Info("lifecycle policy applied", "resource_id", resourceInstanceId, "version", version, "state", state, "version_change", goalState.versionChange)
	return nil
}
