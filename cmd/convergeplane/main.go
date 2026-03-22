package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/convergeplane/convergeplane/internal/config"
	"github.com/convergeplane/convergeplane/internal/server"
)

func main() {
	cfg := config.Load()

	srv := server.New(cfg)

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
