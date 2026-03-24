package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/convergeplane/convergeplane/internal/config"
	"github.com/convergeplane/convergeplane/internal/server"
	"github.com/convergeplane/convergeplane/internal/service"
	"github.com/convergeplane/convergeplane/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := config.Load()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	defer pool.Close()

	tenantGroupRepo := postgres.NewTenantGroupRepo(pool)
	tenantRepo := postgres.NewTenantRepo(pool)

	regSvc := service.NewRegistrationService(tenantGroupRepo, tenantRepo)

	srv := server.New(cfg, regSvc)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		log.Fatalf("server error: %v", err)
	case sig := <-sigCh:
		log.Printf("received signal %v, shutting down", sig)
		srv.Stop()
	}
}
