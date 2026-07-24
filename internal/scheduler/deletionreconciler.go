package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/boundflow/boundflow/internal/storage"
)

// DeletionReconciler finishes workflow deletions left pending by InitiateDelete: any
// request that was in flight when deletion was requested may still be running, or may
// have slipped past the abandon step in a narrow race window. This sweep re-runs the
// same abandon-then-check-then-finalize steps until nothing is left outstanding. A
// partition-scoped PartitionWorker, same pattern as ApprovalTimeoutResolver.
type DeletionReconciler struct {
	interval         int
	workflows        storage.WorkflowRepository
	customerRequests storage.CustomerRequestRepository
	log              *slog.Logger
}

func NewDeletionReconciler(interval int, workflows storage.WorkflowRepository, customerRequests storage.CustomerRequestRepository, log *slog.Logger) *DeletionReconciler {
	return &DeletionReconciler{
		interval:         interval,
		workflows:        workflows,
		customerRequests: customerRequests,
		log:              log.With("component", "deletion_reconciler"),
	}
}

func (r *DeletionReconciler) Run(ctx context.Context, partitionID string) error {
	r.log.Info("deletion reconciler starting", "partition_id", partitionID)
	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sweep(ctx, partitionID)
		case <-ctx.Done():
			r.log.Info("deletion reconciler stopping", "partition_id", partitionID)
			return nil
		}
	}
}

func (r *DeletionReconciler) sweep(ctx context.Context, partitionID string) {
	ids, err := r.workflows.ListPendingDeletion(ctx, partitionID)
	if err != nil {
		r.log.Error("failed to list workflows pending deletion", "partition_id", partitionID, "error", err)
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

func (r *DeletionReconciler) reconcileOne(ctx context.Context, id string) {
	if err := r.customerRequests.AbandonUnscheduledRequests(ctx, id); err != nil {
		r.log.Error("failed to abandon unscheduled requests", "workflow_id", id, "error", err)
		return
	}

	running, err := r.customerRequests.HasRunningRequest(ctx, id)
	if err != nil {
		r.log.Error("failed to check for running request", "workflow_id", id, "error", err)
		return
	}
	if running {
		return
	}

	if err := r.workflows.FinalizeDeleted(ctx, id); err != nil {
		r.log.Error("failed to finalize deletion", "workflow_id", id, "error", err)
		return
	}
	r.log.Info("finalized pending deletion", "workflow_id", id)
}
