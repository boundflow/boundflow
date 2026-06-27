package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
)

// ApprovalTimeoutResolver rejects approval gates whose timeout has passed and
// writes a timed_out audit row for each. A partition-scoped PartitionWorker that
// mirrors LifecycleResolver's cooldown-expiry loop: the scheduler that owns a
// partition runs it, so ownership gives exclusivity (no cross-scheduler locking).
type ApprovalTimeoutResolver struct {
	interval int
	jobs     storage.JobRepository
	audit    storage.AuditRepository
	log      *slog.Logger
}

func NewApprovalTimeoutResolver(interval int, jobs storage.JobRepository, audit storage.AuditRepository, log *slog.Logger) *ApprovalTimeoutResolver {
	return &ApprovalTimeoutResolver{
		interval: interval,
		jobs:     jobs,
		audit:    audit,
		log:      log.With("component", "approval_timeout_resolver"),
	}
}

func (r *ApprovalTimeoutResolver) Run(ctx context.Context, partitionID string) error {
	r.log.Info("approval timeout resolver starting", "partition_id", partitionID)
	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sweep(ctx, partitionID)
		case <-ctx.Done():
			r.log.Info("approval timeout resolver stopping", "partition_id", partitionID)
			return nil
		}
	}
}

func (r *ApprovalTimeoutResolver) sweep(ctx context.Context, partitionID string) {
	expired, err := r.jobs.SweepExpiredApprovals(ctx, partitionID)
	if err != nil {
		r.log.Error("failed to sweep expired approvals", "partition_id", partitionID, "error", err)
		return
	}
	for _, e := range expired {
		decidedAt := e.TimedOutAt
		details, err := json.Marshal(domain.ApprovalAuditDetails{
			ApprovalID: e.ApprovalID,
			OpenedAt:   e.OpenedAt,
			DecidedAt:  &decidedAt,
			Decision:   domain.ApprovalTimedOut,
		})
		if err != nil {
			r.log.Error("failed to marshal timed_out audit details", "approval_id", e.ApprovalID, "error", err)
			continue
		}
		// Timeout has no actor. Best-effort: the gate is already resolved, so a
		// failed audit write is logged, not retried into blocking the sweep.
		if err := r.audit.Append(ctx, domain.AuditEvent{
			TenantGroupID: e.TenantGroupID,
			WorkflowID:    e.WorkflowID,
			RequestID:     e.RequestID,
			EventType:     domain.AuditEventApproval,
			OccurredAt:    decidedAt,
			Details:       details,
		}); err != nil {
			r.log.Error("failed to append timed_out audit", "approval_id", e.ApprovalID, "error", err)
		}
	}
	if len(expired) > 0 {
		r.log.Info("resolved expired approval gates by timeout", "count", len(expired))
	}
}
