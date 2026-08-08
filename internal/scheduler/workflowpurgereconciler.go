package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/boundflow/boundflow/internal/storage"
)

// WorkflowPurgeReconciler hard-deletes the operational rows of workflows that have been
// finalized (lifecycle_state = deleted) for at least purgeAge: customer_requests and
// workflow_version_metrics first, then the workflows row itself last, since its presence
// is exactly the signal ListPurgeable uses to find work. A partition-scoped
// PartitionWorker, same pattern as DeletionReconciler.
type WorkflowPurgeReconciler struct {
	interval         int
	purgeAge         time.Duration
	workflows        storage.WorkflowRepository
	customerRequests storage.CustomerRequestRepository
	versionMetrics   storage.VersionMetricsRepository
	log              *slog.Logger
}

func NewWorkflowPurgeReconciler(interval int, purgeAgeSeconds int, workflows storage.WorkflowRepository, customerRequests storage.CustomerRequestRepository, versionMetrics storage.VersionMetricsRepository, log *slog.Logger) *WorkflowPurgeReconciler {
	return &WorkflowPurgeReconciler{
		interval:         interval,
		purgeAge:         time.Duration(purgeAgeSeconds) * time.Second,
		workflows:        workflows,
		customerRequests: customerRequests,
		versionMetrics:   versionMetrics,
		log:              log.With("component", "workflow_purge_reconciler"),
	}
}

func (r *WorkflowPurgeReconciler) Run(ctx context.Context, partitionID string) error {
	r.log.Info("workflow purge reconciler starting", "partition_id", partitionID)
	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sweep(ctx, partitionID)
		case <-ctx.Done():
			r.log.Info("workflow purge reconciler stopping", "partition_id", partitionID)
			return nil
		}
	}
}

func (r *WorkflowPurgeReconciler) sweep(ctx context.Context, partitionID string) {
	ids, err := r.workflows.ListPurgeable(ctx, partitionID, r.purgeAge)
	if err != nil {
		r.log.Error("failed to list purgeable workflows", "partition_id", partitionID, "error", err)
		return
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			r.reconcileOne(ctx, id)
		}(id)
	}
	wg.Wait()
}

func (r *WorkflowPurgeReconciler) reconcileOne(ctx context.Context, id string) {
	if err := r.customerRequests.DeleteForWorkflow(ctx, id); err != nil {
		r.log.Error("failed to delete customer requests for workflow", "workflow_id", id, "error", err)
		return
	}

	if err := r.versionMetrics.DeleteForWorkflow(ctx, id); err != nil {
		r.log.Error("failed to delete version metrics for workflow", "workflow_id", id, "error", err)
		return
	}

	purged, err := r.workflows.PurgeDeleted(ctx, id)
	if err != nil {
		r.log.Error("failed to purge workflow", "workflow_id", id, "error", err)
		return
	}
	if purged {
		r.log.Info("purged workflow", "workflow_id", id)
	}
}