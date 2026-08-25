package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/boundflow/boundflow/internal/storage"
)

// AbandonedJobResolver finishes runs flagged for abandon that are parked at a gate. It is
// the server-side twin of the worker's dispatch-time check: AcquireJob excludes the awaiting
// states, so no worker can pick these up to honour the flag. A partition-scoped
// PartitionWorker, same shape as ApprovalTimeoutResolver.
type AbandonedJobResolver struct {
	interval int
	jobs     storage.JobRepository
	log      *slog.Logger
}

func NewAbandonedJobResolver(interval int, jobs storage.JobRepository, log *slog.Logger) *AbandonedJobResolver {
	return &AbandonedJobResolver{
		interval: interval,
		jobs:     jobs,
		log:      log.With("component", "abandoned_job_resolver"),
	}
}

func (r *AbandonedJobResolver) Run(ctx context.Context, partitionID string) error {
	r.log.Info("abandoned job resolver starting", "partition_id", partitionID)
	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sweep(ctx, partitionID)
		case <-ctx.Done():
			r.log.Info("abandoned job resolver stopping", "partition_id", partitionID)
			return nil
		}
	}
}

func (r *AbandonedJobResolver) sweep(ctx context.Context, partitionID string) {
	finished, err := r.jobs.SweepAbandonedGates(ctx, partitionID)
	if err != nil {
		r.log.Error("failed to sweep abandoned gates", "partition_id", partitionID, "error", err)
		return
	}
	if len(finished) > 0 {
		r.log.Info("finished abandoned runs parked at a gate", "count", len(finished), "workflow_ids", finished)
	}
}
