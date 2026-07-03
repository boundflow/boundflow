package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boundflow/boundflow/internal/auth"
	"github.com/boundflow/boundflow/internal/config"
	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/metrics"
	"github.com/boundflow/boundflow/internal/rpcworker"
	internalscheduler "github.com/boundflow/boundflow/internal/scheduler"
	"github.com/boundflow/boundflow/internal/server"
	"github.com/boundflow/boundflow/internal/service"
	"github.com/boundflow/boundflow/internal/storage/postgres"
	"github.com/boundflow/boundflow/migrations"
)

func main() {
	mode := flag.String("mode", "", "run mode: server | scheduler | worker | provision")
	name := flag.String("name", "", "provision mode: customer / tenant group name")
	flag.Parse()

	if *mode == "" {
		log.Fatal("usage: boundflow -mode=<server|scheduler|worker|provision>")
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
	case "provision":
		runProvision(*name)
	case "migrate":
		runMigrate()
	default:
		log.Fatalf("unknown mode %q: must be server, scheduler, worker, provision, or migrate", *mode)
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
	requirePositive("BOUNDFLOW_GRPC_PORT", cfg.GRPCPort)
	requirePositive("BOUNDFLOW_NUM_PARTITIONS", cfg.NumPartitions)

	pool := mustConnectDB(cfg.DatabaseURL, logger)
	defer pool.Close()

	tenantGroupRepo := postgres.NewTenantGroupRepo(pool)
	tenantRepo := postgres.NewTenantRepo(pool)
	workflowRepo := postgres.NewWorkflowRepo(pool)
	customerRequestRepo := postgres.NewCustomerRequestRepo(pool)
	agentStateRepo := postgres.NewAgentStateRepo(pool)
	schedulerRepo := postgres.NewSchedulerRepo(pool)
	partitionRepo := postgres.NewSchedulerPartitionRepo(pool)
	jobRepo := postgres.NewJobRepo(pool)
	versionMetricsRepo := postgres.NewVersionMetricsRepo(pool)
	metricsRepo := postgres.NewMetricsRepo(pool)
	lifecycleResolverRepo := postgres.NewLifecycleResolverRepo(pool)
	metricsHandler := metrics.NewMetricsHandler(workflowRepo, agentStateRepo, versionMetricsRepo, metricsRepo, logger)
	auditRepo := postgres.NewAuditRepo(pool)
	policyResolver := internalscheduler.NewLifecycleResolver(30, logger, lifecycleResolverRepo, workflowRepo, versionMetricsRepo)
	sched := internalscheduler.NewScheduler("server", 30, 25, partitionRepo, schedulerRepo, customerRequestRepo, workflowRepo, agentStateRepo, jobRepo, metricsHandler, policyResolver, auditRepo, logger)

	modelPricingRepo := postgres.NewModelPricingRepo(pool)
	regSvc := service.NewRegistrationService(tenantGroupRepo, tenantRepo, modelPricingRepo)
	lifecycleSvc := service.NewLifecycleService(workflowRepo, customerRequestRepo, tenantRepo, tenantGroupRepo, agentStateRepo, modelPricingRepo, sched, sched, auditRepo, cfg.NumPartitions, logger)

	authn := auth.NewAuthenticator(postgres.NewApiKeyRepo(pool))
	srv := server.New(cfg, regSvc, lifecycleSvc, authn, cfg.Debug)

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
	logger.Info("starting scheduler", "num_partitions", cfg.NumPartitions, "max_partitions_per_scheduler", cfg.MaxPartitionsPerScheduler)
	requirePositive("BOUNDFLOW_NUM_PARTITIONS", cfg.NumPartitions)
	requirePositive("BOUNDFLOW_MAX_PARTITIONS_PER_SCHEDULER", cfg.MaxPartitionsPerScheduler)

	pool := mustConnectDB(cfg.DatabaseURL, logger)
	defer pool.Close()

	partitionRepo := postgres.NewSchedulerPartitionRepo(pool)
	customerRequestRepo := postgres.NewCustomerRequestRepo(pool)
	workflowRepo := postgres.NewWorkflowRepo(pool)
	agentStateRepo := postgres.NewAgentStateRepo(pool)
	schedulerRepo := postgres.NewSchedulerRepo(pool)
	jobRepo := postgres.NewJobRepo(pool)
	versionMetricsRepo := postgres.NewVersionMetricsRepo(pool)
	metricsRepo := postgres.NewMetricsRepo(pool)
	lifecycleResolverRepo := postgres.NewLifecycleResolverRepo(pool)
	tenantRepo := postgres.NewTenantRepo(pool)
	tenantGroupRepo := postgres.NewTenantGroupRepo(pool)
	modelPricingRepo := postgres.NewModelPricingRepo(pool)
	metricsHandler := metrics.NewMetricsHandler(workflowRepo, agentStateRepo, versionMetricsRepo, metricsRepo, logger)

	auditRepo := postgres.NewAuditRepo(pool)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	// One goroutine per partition this scheduler will hold.
	for i := range cfg.MaxPartitionsPerScheduler {
		schedulerID := uuid.NewString()
		resolver := internalscheduler.NewLifecycleResolver(30, logger, lifecycleResolverRepo, workflowRepo, versionMetricsRepo)
		sched := internalscheduler.NewScheduler(schedulerID, 30, 25, partitionRepo, schedulerRepo, customerRequestRepo, workflowRepo, agentStateRepo, jobRepo, metricsHandler, resolver, auditRepo, logger)
		// The periodic handler invokes due workflows via the scheduler + lifecycle service.
		lifecycleSvc := service.NewLifecycleService(workflowRepo, customerRequestRepo, tenantRepo, tenantGroupRepo, agentStateRepo, modelPricingRepo, sched, sched, auditRepo, cfg.NumPartitions, logger)
		periodic := internalscheduler.NewPeriodicWorkflowHandler(30, logger, sched, lifecycleSvc, schedulerRepo, customerRequestRepo)
		// Approval-gate timeouts resolve here (partition-scoped, like cooldown expiry)
		approvalTimeouts := internalscheduler.NewApprovalTimeoutResolver(30, jobRepo, auditRepo, logger)
		// Resolver (cooldown expiry) and periodic handler are partition-scoped: the scheduler
		// starts them when it acquires a partition and cancels them when it loses it.
		sched.SetPartitionWorkers(resolver, periodic, approvalTimeouts)
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
	logger.Info("starting worker", "grpc_port", cfg.WorkerGRPCPort)
	requirePositive("BOUNDFLOW_WORKER_GRPC_PORT", cfg.WorkerGRPCPort)
	requirePositive("BOUNDFLOW_JOB_TIMEOUT_SECS", cfg.JobTimeoutSecs)

	pool := mustConnectDB(cfg.DatabaseURL, logger)
	defer pool.Close()

	jobRepo := postgres.NewJobRepo(pool)
	customerRequestRepo := postgres.NewCustomerRequestRepo(pool)
	workflowRepo := postgres.NewWorkflowRepo(pool)
	// partitionRepo is passed to satisfy the scheduler constructor but the worker scheduler
	// only calls CompleteRequest/FailRequest — the partition table is never queried.
	partitionRepo := postgres.NewSchedulerPartitionRepo(pool)
	agentStateRepo := postgres.NewAgentStateRepo(pool)
	schedulerRepo := postgres.NewSchedulerRepo(pool)

	versionMetricsRepo := postgres.NewVersionMetricsRepo(pool)
	metricsRepo := postgres.NewMetricsRepo(pool)
	lifecycleResolverRepo := postgres.NewLifecycleResolverRepo(pool)
	metricsHandler := metrics.NewMetricsHandler(workflowRepo, agentStateRepo, versionMetricsRepo, metricsRepo, logger)

	workerID := uuid.NewString()
	auditRepo := postgres.NewAuditRepo(pool)
	policyResolver := internalscheduler.NewLifecycleResolver(30, logger, lifecycleResolverRepo, workflowRepo, versionMetricsRepo)
	sched := internalscheduler.NewScheduler(workerID, 30, 25, partitionRepo, schedulerRepo, customerRequestRepo, workflowRepo, agentStateRepo, jobRepo, metricsHandler, policyResolver, auditRepo, logger)

	worker := rpcworker.NewRpcWorker(jobRepo, auditRepo, workerID, cfg.JobTimeoutSecs, sched, metricsHandler, logger)
	authn := auth.NewAuthenticator(postgres.NewApiKeyRepo(pool))
	srv := server.NewWorkerServer(cfg, worker, authn)

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

// runProvision creates a tenant group and API key for a new customer, then prints
// the raw key. The key is hashed before storage and cannot be recovered afterwards.
// Self-hosters run this once against the running stack:
//
//	docker compose run --rm server -mode=provision -name=me
func runProvision(name string) {
	if name == "" {
		log.Fatal("usage: boundflow -mode=provision -name=<customer-name>")
	}

	cfg := config.LoadScheduler()
	logger := newLogger(cfg.LogLevel)

	pool := mustConnectDB(cfg.DatabaseURL, logger)
	defer pool.Close()

	ctx := context.Background()

	group := &domain.TenantGroup{
		ID:        uuid.New().String(),
		Name:      name,
		CreatedAt: time.Now(),
	}
	if err := postgres.NewTenantGroupRepo(pool).Create(ctx, group); err != nil {
		log.Fatalf("create tenant group: %v", err)
	}

	// Cryptographically random key (~40 URL-safe chars); only its hash is stored.
	raw, err := generateKey()
	if err != nil {
		log.Fatalf("generate api key: %v", err)
	}

	apiKey := &domain.ApiKey{
		ID:            uuid.New().String(),
		KeyHash:       auth.HashKey(raw),
		TenantGroupID: group.ID,
		CreatedAt:     time.Now(),
	}
	if err := postgres.NewApiKeyRepo(pool).Create(ctx, apiKey); err != nil {
		log.Fatalf("create api key: %v", err)
	}

	fmt.Printf("tenant_group_id  : %s\n", group.ID)
	fmt.Printf("tenant_group_name: %s\n", group.Name)
	fmt.Printf("api_key_id       : %s\n", apiKey.ID)
	fmt.Printf("api_key          : %s\n", raw)
	fmt.Println()
	fmt.Println("Set this as BOUNDFLOW_API_KEY for the SDK. It is not stored and cannot be recovered.")
}

// runMigrate applies all pending database migrations. The SQL files are embedded
// in the binary (see the migrations package), so the distributed image carries
// its own schema and needs no migrations directory mounted alongside it.
func runMigrate() {
	cfg := config.LoadScheduler()
	logger := newLogger(cfg.LogLevel)
	logger.Info("running migrations")
	requirePositive("BOUNDFLOW_NUM_PARTITIONS", cfg.NumPartitions)

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		log.Fatalf("load embedded migrations: %v", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("init migrate: %v", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("apply migrations: %v", err)
	}
	logger.Info("migrations applied")

	// Seed scheduler partitions [0, NUM_PARTITIONS) as part of one-time DB setup.
	pool := mustConnectDB(cfg.DatabaseURL, logger)
	defer pool.Close()
	if err := postgres.NewSchedulerPartitionRepo(pool).SeedPartitions(context.Background(), cfg.NumPartitions); err != nil {
		log.Fatalf("seed scheduler partitions: %v", err)
	}
	logger.Info("scheduler partitions seeded", "num_partitions", cfg.NumPartitions)
}

func generateKey() (string, error) {
	b := make([]byte, 30)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// requirePositive fails fast if a required integer config var is unset or <= 0.
func requirePositive(envVar string, v int) {
	if v < 1 {
		log.Fatalf("%s must be set to a positive integer", envVar)
	}
}

func mustConnectDB(url string, logger *slog.Logger) *pgxpool.Pool {
	if url == "" {
		log.Fatal("BOUNDFLOW_DATABASE_URL must be set")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		logger.Error("unable to connect to database", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to database")
	return pool
}
