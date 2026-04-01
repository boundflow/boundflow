package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/config"
	"github.com/convergeplane/convergeplane/internal/server"
	"github.com/convergeplane/convergeplane/internal/service"
	"github.com/convergeplane/convergeplane/internal/storage/postgres"
)

func main() {
	mode := flag.String("mode", "", "run mode: server | scheduler | worker")
	flag.Parse()

	if *mode == "" {
		log.Fatal("usage: convergeplane -mode=<server|scheduler|worker>")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	switch *mode {
	case "server":
		runServer(sigCh)
	case "scheduler":
		runScheduler(sigCh)
	case "worker":
		runWorker(sigCh)
	default:
		log.Fatalf("unknown mode %q: must be server, scheduler, or worker", *mode)
	}
}

func runServer(sigCh <-chan os.Signal) {
	cfg := config.LoadServer()

	pool := mustConnectDB(cfg.DatabaseURL)
	defer pool.Close()

	tenantGroupRepo := postgres.NewTenantGroupRepo(pool)
	tenantRepo := postgres.NewTenantRepo(pool)
	resourceInstanceRepo := postgres.NewResourceInstanceRepo(pool)
	customerRequestRepo := postgres.NewCustomerRequestRepo(pool)
	schedulerRepo := postgres.NewSchedulerRepo(pool)

	regSvc := service.NewRegistrationService(tenantGroupRepo, tenantRepo)
	lifecycleSvc := service.NewLifecycleService(resourceInstanceRepo, customerRequestRepo, schedulerRepo)

	srv := server.New(cfg, regSvc, lifecycleSvc)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()

	select {
	case err := <-errCh:
		log.Fatalf("server error: %v", err)
	case sig := <-sigCh:
		log.Printf("received signal %v, shutting down", sig)
		srv.Stop()
	}
}

func runScheduler(sigCh <-chan os.Signal) {
	cfg := config.LoadScheduler()
	log.Printf("starting scheduler with %d partition(s)", cfg.NumPartitions)

	// TODO: implement scheduler
	<-sigCh
	log.Println("scheduler shutting down")
}

func runWorker(sigCh <-chan os.Signal) {
	cfg := config.LoadWorker()
	log.Printf("starting worker with %d worker(s)", cfg.NumWorkers)

	// TODO: implement worker
	<-sigCh
	log.Println("worker shutting down")
}

func mustConnectDB(url string) *pgxpool.Pool {
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	return pool
}
