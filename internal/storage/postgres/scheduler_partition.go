package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boundflow/boundflow/internal/domain"
)

type SchedulerPartitionRepo struct {
	pool *pgxpool.Pool
}

func NewSchedulerPartitionRepo(pool *pgxpool.Pool) *SchedulerPartitionRepo {
	return &SchedulerPartitionRepo{pool: pool}
}

func (r *SchedulerPartitionRepo) AcquireAvailable(ctx context.Context, ownerID string, leaseDuration time.Duration) (*domain.SchedulerPartition, error) {
	var p domain.SchedulerPartition
	err := r.pool.QueryRow(ctx,
		`UPDATE scheduler_partitions
		 SET owner = $1, lease_until = now() + $2::interval
		 WHERE id = (
		   SELECT id FROM scheduler_partitions
		   WHERE owner IS NULL OR owner = '' OR lease_until < now()
		   LIMIT 1
		   FOR UPDATE SKIP LOCKED
		 )
		 RETURNING id, resource_instance_count, owner, lease_until`,
		ownerID, leaseDuration.String(),
	).Scan(&p.ID, &p.ResourceInstanceCount, &p.Owner, &p.LeaseUntil)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, handleError(err, "scheduler partition")
	}
	return &p, nil
}

func (r *SchedulerPartitionRepo) Renew(ctx context.Context, partitionID string, ownerID string, leaseDuration time.Duration) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE scheduler_partitions
		 SET owner = $1, lease_until = now() + $2::interval
		 WHERE id = $3 AND (owner = $1 OR owner IS NULL OR owner = '' OR lease_until < now())`,
		ownerID, leaseDuration.String(), partitionID,
	)
	if err != nil {
		return false, handleError(err, "scheduler partition")
	}
	return tag.RowsAffected() == 1, nil
}

func (r *SchedulerPartitionRepo) Release(ctx context.Context, partitionID string, ownerID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE scheduler_partitions
		 SET owner = '', lease_until = NULL
		 WHERE id = $1 AND owner = $2`,
		partitionID, ownerID,
	)
	if err != nil {
		return handleError(err, "scheduler partition")
	}
	return nil
}
