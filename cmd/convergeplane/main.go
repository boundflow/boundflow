package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/config"
	"github.com/convergeplane/convergeplane/internal/rpcworker"
	internalscheduler "github.com/convergeplane/convergeplane/internal/scheduler"
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

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func runServer(sigCh <-chan os.Signal) {
	cfg := config.LoadServer()
	logger := newLogger(cfg.LogLevel)
	logger.Info("starting server", "grpc_port", cfg.GRPCPort)

	pool := mustConnectDB(cfg.DatabaseURL, logger)
	defer pool.Close()

	tenantGroupRepo := postgres.NewTenantGroupRepo(pool)
	tenantRepo := postgres.NewTenantRepo(pool)
	resourceInstanceRepo := postgres.NewResourceInstanceRepo(pool)
	customerRequestRepo := postgres.NewCustomerRequestRepo(pool)
	schedulerRepo := postgres.NewSchedulerRepo(pool)
	partitionRepo := postgres.NewSchedulerPartitionRepo(pool)

	sched := internalscheduler.New("server", 30, partitionRepo, schedulerRepo, customerRequestRepo, resourceInstanceRepo, logger)

	regSvc := service.NewRegistrationService(tenantGroupRepo, tenantRepo)
	lifecycleSvc := service.NewLifecycleService(resourceInstanceRepo, customerRequestRepo, sched, logger)

	srv := server.New(cfg, regSvc, lifecycleSvc, cfg.Debug)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()

	select {
	case err := <-errCh:
		logger.Error("server error", "error", err)
		os.Exit(1)
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", "signal", sig)
		srv.Stop()
	}
}

func runScheduler(sigCh <-chan os.Signal) {
	cfg := config.LoadScheduler()
	logger := newLogger(cfg.LogLevel)
	logger.Info("starting scheduler", "num_partitions", cfg.NumPartitions)

	pool := mustConnectDB(cfg.DatabaseURL, logger)
	defer pool.Close()

	partitionRepo := postgres.NewSchedulerPartitionRepo(pool)
	schedulerRepo := postgres.NewSchedulerRepo(pool)
	customerRequestRepo := postgres.NewCustomerRequestRepo(pool)
	resourceInstanceRepo := postgres.NewResourceInstanceRepo(pool)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	for i := range cfg.NumPartitions {
		schedulerID := uuid.NewString()
		sched := internalscheduler.New(schedulerID, 30, partitionRepo, schedulerRepo, customerRequestRepo, resourceInstanceRepo, logger)
		logger.Info("starting scheduler partition worker", "index", i, "scheduler_id", schedulerID)
		go func() { errCh <- sched.Run(ctx) }()
	}

	select {
	case err := <-errCh:
		cancel()
		logger.Error("scheduler error", "error", err)
		os.Exit(1)
	case sig := <-sigCh:
		cancel()
		logger.Info("received signal, shutting down", "signal", sig)
	}
}

func runWorker(sigCh <-chan os.Signal) {
	cfg := config.LoadWorker()
	logger := newLogger(cfg.LogLevel)
	logger.Info("starting worker", "num_workers", cfg.NumWorkers, "grpc_port", cfg.WorkerGRPCPort)

	pool := mustConnectDB(cfg.DatabaseURL, logger)
	defer pool.Close()

	jobRepo := postgres.NewJobRepo(pool)
	schedulerRepo := postgres.NewSchedulerRepo(pool)
	customerRequestRepo := postgres.NewCustomerRequestRepo(pool)
	resourceInstanceRepo := postgres.NewResourceInstanceRepo(pool)
	// partitionRepo is passed to satisfy the scheduler constructor but the worker scheduler
	// only calls CompleteRequest/FailRequest — the partition table is never queried.
	partitionRepo := postgres.NewSchedulerPartitionRepo(pool)

	workerID := uuid.NewString()
	sched := internalscheduler.New(workerID, 30, partitionRepo, schedulerRepo, customerRequestRepo, resourceInstanceRepo, logger)

	worker := rpcworker.NewRpcWorker(jobRepo, workerID, cfg.JobTimeoutSecs, sched)
	srv := server.NewWorkerServer(cfg, worker)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()

	select {
	case err := <-errCh:
		logger.Error("worker server error", "error", err)
		os.Exit(1)
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", "signal", sig)
		srv.Stop()
	}
}

func mustConnectDB(url string, logger *slog.Logger) *pgxpool.Pool {
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		logger.Error("unable to connect to database", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to database")
	return pool
}
