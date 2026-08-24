package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
)

// SuspensionReconciler finishes suspensions left pending by SuspendWorkflow: the run in
// flight when one was requested has to end before the workflow is really stopped, and
// any of the inline steps may have failed. This sweep re-runs the same
// hold-then-stop-then-check-then-finalize steps until nothing is left outstanding. A
// partition-scoped PartitionWorker, same pattern as DeletionReconciler.
//
// The operator's choices are read from the workflow row rather than passed in, so a
// re-run takes the same steps the inline attempt did.
type SuspensionReconciler struct {
	interval         int
	workflows        storage.WorkflowRepository
	customerRequests storage.CustomerRequestRepository
	jobs             storage.JobRepository
	log              *slog.Logger
}

func NewSuspensionReconciler(interval int, workflows storage.WorkflowRepository, customerRequests storage.CustomerRequestRepository, jobs storage.JobRepository, log *slog.Logger) *SuspensionReconciler {
	return &SuspensionReconciler{
		interval:         interval,
		workflows:        workflows,
		customerRequests: customerRequests,
		jobs:             jobs,
		log:              log.With("component", "suspension_reconciler"),
	}
}

func (r *SuspensionReconciler) Run(ctx context.Context, partitionID string) error {
	r.log.Info("suspension reconciler starting", "partition_id", partitionID)
	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sweep(ctx, partitionID)
		case <-ctx.Done():
			r.log.Info("suspension reconciler stopping", "partition_id", partitionID)
			return nil
		}
	}
}

func (r *SuspensionReconciler) sweep(ctx context.Context, partitionID string) {
	workflows, err := r.workflows.ListPendingSuspension(ctx, partitionID)
	if err != nil {
		r.log.Error("failed to list workflows pending suspension", "partition_id", partitionID, "error", err)
		return
	}

	var wg sync.WaitGroup
	for _, w := range workflows {
		wg.Add(1)
		go func(w *domain.Workflow) {
			defer wg.Done()
			r.reconcileOne(ctx, w)
		}(w)
	}
	wg.Wait()
}

func (r *SuspensionReconciler) reconcileOne(ctx context.Context, workflow *domain.Workflow) {
	id := workflow.ID

	if err := r.customerRequests.SuspendUnscheduledRequests(ctx, id, workflow.Suspension.AbandonQueued); err != nil {
		r.log.Error("failed to hold queued requests", "workflow_id", id, "error", err)
		return
	}

	if workflow.Suspension.StopCurrent {
		if _, err := r.jobs.MarkAbandonRequested(ctx, id); err != nil {
			r.log.Error("failed to request job abandon", "workflow_id", id, "error", err)
			return
		}
	}

	running, err := r.customerRequests.HasRunningRequest(ctx, id)
	if err != nil {
		r.log.Error("failed to check for running request", "workflow_id", id, "error", err)
		return
	}
	if running {
		return
	}

	if err := r.workflows.FinalizeSuspended(ctx, id); err != nil {
		r.log.Error("failed to finalize suspension", "workflow_id", id, "error", err)
		return
	}
	r.log.Info("finalized pending suspension", "workflow_id", id, "suspension_id", workflow.Suspension.ID)
}
