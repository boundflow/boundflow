package config

import (
	"os"
	"strconv"
)

type Config struct {
	GRPCPort int
	LogLevel string
}

func Load() *Config {
	cfg := &Config{
		GRPCPort: 50051,
		LogLevel: "info",
	}

	if v := os.Getenv("CONVERGEPLANE_GRPC_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.GRPCPort = port
		}
	}

	if v := os.Getenv("CONVERGEPLANE_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}

	return cfg
}
