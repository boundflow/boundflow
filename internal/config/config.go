package config

import (
	"os"
	"strconv"
)

type Config struct {
	GRPCPort    int
	LogLevel    string
	DatabaseURL string
}

func Load() *Config {
	cfg := &Config{
		GRPCPort:    50051,
		LogLevel:    "info",
		DatabaseURL: "postgres://localhost:5432/convergeplane?sslmode=disable",
	}

	if v := os.Getenv("CONVERGEPLANE_GRPC_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.GRPCPort = port
		}
	}

	if v := os.Getenv("CONVERGEPLANE_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}

	if v := os.Getenv("CONVERGEPLANE_DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}

	return cfg
}
