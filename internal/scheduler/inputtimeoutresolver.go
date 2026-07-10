package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
)

// InputTimeoutResolver times out input gates whose deadline has passed and writes a
// timed_out audit row for each. A partition-scoped PartitionWorker that mirrors
// ApprovalTimeoutResolver: the scheduler that owns a partition runs it, so ownership
// gives exclusivity (no cross-scheduler locking).
type InputTimeoutResolver struct {
	interval int
	jobs     storage.JobRepository
	audit    storage.AuditRepository
	log      *slog.Logger
}

func NewInputTimeoutResolver(interval int, jobs storage.JobRepository, audit storage.AuditRepository, log *slog.Logger) *InputTimeoutResolver {
	return &InputTimeoutResolver{
		interval: interval,
		jobs:     jobs,
		audit:    audit,
		log:      log.With("component", "input_timeout_resolver"),
	}
}

func (r *InputTimeoutResolver) Run(ctx context.Context, partitionID string) error {
	r.log.Info("input timeout resolver starting", "partition_id", partitionID)
	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sweep(ctx, partitionID)
		case <-ctx.Done():
			r.log.Info("input timeout resolver stopping", "partition_id", partitionID)
			return nil
		}
	}
}

func (r *InputTimeoutResolver) sweep(ctx context.Context, partitionID string) {
	expired, err := r.jobs.SweepExpiredInputs(ctx, partitionID)
	if err != nil {
		r.log.Error("failed to sweep expired inputs", "partition_id", partitionID, "error", err)
		return
	}
	for _, e := range expired {
		decidedAt := e.TimedOutAt
		details, err := json.Marshal(domain.InputAuditDetails{
			InputID:   e.InputID,
			OpenedAt:  e.OpenedAt,
			DecidedAt: &decidedAt,
			Decision:  domain.InputTimedOut,
		})
		if err != nil {
			r.log.Error("failed to marshal timed_out audit details", "input_id", e.InputID, "error", err)
			continue
		}
		// Timeout has no actor. Best-effort: the gate is already resolved, so a
		// failed audit write is logged, not retried into blocking the sweep.
		if err := r.audit.Append(ctx, domain.AuditEvent{
			TenantGroupID: e.TenantGroupID,
			WorkflowID:    e.WorkflowID,
			RequestID:     e.RequestID,
			EventType:     domain.AuditEventInput,
			OccurredAt:    decidedAt,
			Details:       details,
		}); err != nil {
			r.log.Error("failed to append timed_out audit", "input_id", e.InputID, "error", err)
		}
	}
	if len(expired) > 0 {
		r.log.Info("resolved expired input gates by timeout", "count", len(expired))
	}
}
