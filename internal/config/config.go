package config

import (
	"os"
	"strconv"
)

type BaseConfig struct {
	LogLevel                  string
	DatabaseURL               string
	Debug                     bool
	NumPartitions             int
	MaxPartitionsPerScheduler int
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
	WorkerGRPCPort int
	JobTimeoutSecs int
}

// Config is read entirely from the environment — no values are defaulted in Go.
// Deployments set every var (see docker-compose); each mode validates the vars it
// requires at startup (see cmd/boundflow), so misconfiguration fails fast.

func loadBase() BaseConfig {
	return BaseConfig{
		LogLevel:                  os.Getenv("BOUNDFLOW_LOG_LEVEL"),
		DatabaseURL:               os.Getenv("BOUNDFLOW_DATABASE_URL"),
		Debug:                     os.Getenv("BOUNDFLOW_DEBUG") == "true",
		NumPartitions:             envInt("BOUNDFLOW_NUM_PARTITIONS"),
		MaxPartitionsPerScheduler: envInt("BOUNDFLOW_MAX_PARTITIONS_PER_SCHEDULER"),
	}
}

func LoadServer() *ServerConfig {
	return &ServerConfig{
		BaseConfig: loadBase(),
		GRPCPort:   envInt("BOUNDFLOW_GRPC_PORT"),
	}
}

func LoadScheduler() *SchedulerConfig {
	return &SchedulerConfig{BaseConfig: loadBase()}
}

func LoadWorker() *WorkerConfig {
	return &WorkerConfig{
		BaseConfig:     loadBase(),
		WorkerGRPCPort: envInt("BOUNDFLOW_WORKER_GRPC_PORT"),
		JobTimeoutSecs: envInt("BOUNDFLOW_JOB_TIMEOUT_SECS"),
	}
}

// envInt returns the integer value of key, or 0 if unset or unparseable.
func envInt(key string) int {
	n, _ := strconv.Atoi(os.Getenv(key))
	return n
}
