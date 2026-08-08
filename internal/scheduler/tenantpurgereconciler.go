package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/boundflow/boundflow/internal/storage"
)

// TenantPurgeReconciler hard-deletes soft-deleted tenants once their workflows have all
// been purged. A partition-scoped PartitionWorker, same pattern as DeletionReconciler,
// now that tenants carry scheduler_partition_id too.
type TenantPurgeReconciler struct {
	interval int
	tenants  storage.TenantRepository
	log      *slog.Logger
}

func NewTenantPurgeReconciler(interval int, tenants storage.TenantRepository, log *slog.Logger) *TenantPurgeReconciler {
	return &TenantPurgeReconciler{
		interval: interval,
		tenants:  tenants,
		log:      log.With("component", "tenant_purge_reconciler"),
	}
}

func (r *TenantPurgeReconciler) Run(ctx context.Context, partitionID string) error {
	r.log.Info("tenant purge reconciler starting", "partition_id", partitionID)
	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sweep(ctx, partitionID)
		case <-ctx.Done():
			r.log.Info("tenant purge reconciler stopping", "partition_id", partitionID)
			return nil
		}
	}
}

func (r *TenantPurgeReconciler) sweep(ctx context.Context, partitionID string) {
	ids, err := r.tenants.ListPurgeable(ctx, partitionID)
	if err != nil {
		r.log.Error("failed to list purgeable tenants", "partition_id", partitionID, "error", err)
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

func (r *TenantPurgeReconciler) reconcileOne(ctx context.Context, id string) {
	purged, err := r.tenants.PurgeIfEmpty(ctx, id)
	if err != nil {
		r.log.Error("failed to purge tenant", "tenant_id", id, "error", err)
		return
	}
	if purged {
		r.log.Info("purged tenant", "tenant_id", id)
	}
}
