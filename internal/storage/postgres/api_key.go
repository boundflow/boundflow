package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/domain"
)

type ApiKeyRepo struct {
	pool *pgxpool.Pool
}

func NewApiKeyRepo(pool *pgxpool.Pool) *ApiKeyRepo {
	return &ApiKeyRepo{pool: pool}
}

func (r *ApiKeyRepo) Create(ctx context.Context, key *domain.ApiKey) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO api_keys (id, key_hash, tenant_group_id, created_at)
		 VALUES ($1, $2, $3, $4)`,
		key.ID, key.KeyHash, key.TenantGroupID, key.CreatedAt,
	)
	if err != nil {
		return handleError(err, "api key")
	}
	return nil
}

func (r *ApiKeyRepo) GetByKeyHash(ctx context.Context, keyHash string) (*domain.ApiKey, error) {
	var key domain.ApiKey
	err := r.pool.QueryRow(ctx,
		`SELECT id, key_hash, tenant_group_id, created_at, revoked_at
		 FROM api_keys
		 WHERE key_hash = $1 AND revoked_at IS NULL`,
		keyHash,
	).Scan(&key.ID, &key.KeyHash, &key.TenantGroupID, &key.CreatedAt, &key.RevokedAt)
	if err != nil {
		return nil, handleError(err, "api key")
	}
	return &key, nil
}

func (r *ApiKeyRepo) Revoke(ctx context.Context, id string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`,
		id,
	)
	if err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return handleError(nil, "api key")
	}
	return nil
}
