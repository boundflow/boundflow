package domain

import "time"

type ApiKey struct {
	ID            string
	KeyHash       string
	TenantGroupID string
	CreatedAt     time.Time
	RevokedAt     *time.Time
}
