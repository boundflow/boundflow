// provision_customer creates a tenant group and API key for a new customer.
// Usage: go run ./scripts/provision_customer -name=<customer-name> -db=<postgres-url>
// Prints the raw API key — store it securely; it cannot be recovered after this command exits.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/auth"
	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage/postgres"
)

func main() {
	name := flag.String("name", "", "customer / tenant group name (required)")
	dbURL := flag.String("db", "", "postgres connection URL (required)")
	flag.Parse()

	if *name == "" || *dbURL == "" {
		flag.Usage()
		log.Fatal("both -name and -db are required")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, *dbURL)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	// Create tenant group.
	tenantGroupRepo := postgres.NewTenantGroupRepo(pool)
	group := &domain.TenantGroup{
		ID:        uuid.New().String(),
		Name:      *name,
		CreatedAt: time.Now(),
	}
	if err := tenantGroupRepo.Create(ctx, group); err != nil {
		log.Fatalf("create tenant group: %v", err)
	}

	// Generate a cryptographically random API key (~40 URL-safe chars).
	raw, err := generateKey()
	if err != nil {
		log.Fatalf("generate api key: %v", err)
	}

	// Store the hash (never the raw key).
	apiKeyRepo := postgres.NewApiKeyRepo(pool)
	apiKey := &domain.ApiKey{
		ID:            uuid.New().String(),
		KeyHash:       auth.HashKey(raw),
		TenantGroupID: group.ID,
		CreatedAt:     time.Now(),
	}
	if err := apiKeyRepo.Create(ctx, apiKey); err != nil {
		log.Fatalf("create api key: %v", err)
	}

	fmt.Printf("tenant_group_id : %s\n", group.ID)
	fmt.Printf("tenant_group_name: %s\n", group.Name)
	fmt.Printf("api_key_id      : %s\n", apiKey.ID)
	fmt.Printf("api_key         : %s\n", raw)
	fmt.Println()
	fmt.Println("Send the api_key value to the customer. It is not stored and cannot be recovered.")
}

func generateKey() (string, error) {
	b := make([]byte, 30)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
