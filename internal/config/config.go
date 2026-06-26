package config

import (
	"os"
	"strconv"
)

type BaseConfig struct {
	LogLevel      string
	DatabaseURL   string
	Debug         bool
	NumPartitions int
}

type ServerConfig struct {
	BaseConfig
	GRPCPort int
}

type SchedulerConfig struct {
	BaseConfig
}

type WorkerConfig struct {
	BaseConfig
	NumWorkers      int
	WorkerGRPCPort  int
	JobTimeoutSecs  int
}

func loadBase() BaseConfig {
	base := BaseConfig{
		LogLevel:    "info",
		DatabaseURL: "postgres://localhost:5432/boundflow?sslmode=disable",
	}
	if v := os.Getenv("BOUNDFLOW_LOG_LEVEL"); v != "" {
		base.LogLevel = v
	}
	if v := os.Getenv("BOUNDFLOW_DATABASE_URL"); v != "" {
		base.DatabaseURL = v
	}
	if os.Getenv("BOUNDFLOW_DEBUG") == "true" {
		base.Debug = true
	}
	if v := os.Getenv("BOUNDFLOW_NUM_PARTITIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			base.NumPartitions = n
		}
	}
	return base
}

func LoadServer() *ServerConfig {
	cfg := &ServerConfig{
		BaseConfig: loadBase(),
		GRPCPort:   50051,
	}
	if v := os.Getenv("BOUNDFLOW_GRPC_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.GRPCPort = port
		}
	}
	return cfg
}

func LoadScheduler() *SchedulerConfig {
	return &SchedulerConfig{
		BaseConfig: loadBase(),
	}
}

func LoadWorker() *WorkerConfig {
	cfg := &WorkerConfig{
		BaseConfig:     loadBase(),
		NumWorkers:     1,
		WorkerGRPCPort: 50052,
		JobTimeoutSecs: 300,
	}
	if v := os.Getenv("BOUNDFLOW_NUM_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NumWorkers = n
		}
	}
	if v := os.Getenv("BOUNDFLOW_WORKER_GRPC_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.WorkerGRPCPort = port
		}
	}
	if v := os.Getenv("BOUNDFLOW_JOB_TIMEOUT_SECS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.JobTimeoutSecs = n
		}
	}
	return cfg
}
