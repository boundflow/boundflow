package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/boundflow/boundflow/internal/storage"
)

// AbandonedJobResolver finishes runs flagged for abandon that no worker holds — parked at a
// gate, waiting out a Next delay, or queued with nobody connected. The server-side twin of
// the worker's dispatch-time check. PartitionWorker, same shape as ApprovalTimeoutResolver.
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
	finished, err := r.jobs.SweepAbandonedJobs(ctx, partitionID)
	if err != nil {
		r.log.Error("failed to sweep abandoned jobs", "partition_id", partitionID, "error", err)
		return
	}
	if len(finished) > 0 {
		r.log.Info("finished abandoned runs no worker held", "count", len(finished), "workflow_ids", finished)
	}
}
