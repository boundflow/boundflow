package postgres

import (
	"context"
	"errors"
	"fmt"
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

// SeedPartitions creates the partition rows [0, numPartitions) if missing, making
// NUM_PARTITIONS the single source of truth for the shard count. INSERT-only — it
// never removes partitions, so resharding an existing DB is a separate operation.
func (r *SchedulerPartitionRepo) SeedPartitions(ctx context.Context, numPartitions int) error {
	if numPartitions < 1 {
		return fmt.Errorf("numPartitions must be >= 1, got %d", numPartitions)
	}
	if _, err := r.pool.Exec(ctx,
		`INSERT INTO scheduler_partitions (id)
		 SELECT g::text FROM generate_series(0, $1 - 1) AS g
		 ON CONFLICT (id) DO NOTHING`, numPartitions); err != nil {
		return fmt.Errorf("seed partitions: %w", err)
	}
	return nil
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
		 RETURNING id, workflow_count, owner, lease_until`,
		ownerID, leaseDuration.String(),
	).Scan(&p.ID, &p.WorkflowCount, &p.Owner, &p.LeaseUntil)
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
