package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
)

// SuspensionReconciler finishes suspensions left pending by SuspendWorkflow, once the run in
// flight has ended. PartitionWorker, same shape as DeletionReconciler.
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

// Re-runs the tail from the abandon step. The queue freeze is not repeated: nothing can
// become schedulable again while the workflow stays unrunnable.
func (r *SuspensionReconciler) reconcileOne(ctx context.Context, workflow *domain.Workflow) {
	id := workflow.ID
	suspensionID := workflow.Suspension.ID

	// Delete took the workflow over mid-drain (an interruption can't reach here -- it tears
	// the suspension down itself). Drop the hold rather than sweeping it forever.
	if workflow.WorkflowState != domain.WorkflowStateSuspended {
		if err := r.workflows.AbortSuspension(ctx, id, suspensionID); err != nil {
			r.log.Error("failed to abort suspension", "workflow_id", id, "suspension_id", suspensionID, "error", err)
			return
		}
		r.log.Info("aborted suspension, workflow taken over", "workflow_id", id, "suspension_id", suspensionID, "workflow_state", workflow.WorkflowState)
		return
	}

	if workflow.Suspension.StopCurrent {
		if _, err := r.jobs.MarkAbandonRequested(ctx, id, suspensionID); err != nil {
			r.log.Error("failed to request job abandon", "workflow_id", id, "suspension_id", suspensionID, "error", err)
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

	if err := r.workflows.FinalizeSuspended(ctx, id, suspensionID); err != nil {
		r.log.Error("failed to finalize suspension", "workflow_id", id, "suspension_id", suspensionID, "error", err)
		return
	}
	r.log.Info("finalized pending suspension", "workflow_id", id, "suspension_id", suspensionID)
}
