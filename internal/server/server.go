package server

import (
	"fmt"
	"log"
	"net"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/config"
	"github.com/convergeplane/convergeplane/internal/server/handlers"
	"github.com/convergeplane/convergeplane/internal/service"
	"google.golang.org/grpc"
)

type Server struct {
	grpcServer *grpc.Server
	cfg        *config.Config
}

func New(cfg *config.Config, regSvc *service.RegistrationService) *Server {
	grpcServer := grpc.NewServer()

	convergeplanev1.RegisterRegistrationServiceServer(grpcServer, handlers.NewRegistrationHandler(regSvc))
	convergeplanev1.RegisterResourceLifecycleServiceServer(grpcServer, handlers.NewResourceLifecycleHandler())
	convergeplanev1.RegisterWorkerServiceServer(grpcServer, handlers.NewWorkerHandler())

	return &Server{
		grpcServer: grpcServer,
		cfg:        cfg,
	}
}

func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.cfg.GRPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	log.Printf("convergeplane gRPC server listening on %s", addr)
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	log.Println("shutting down gRPC server...")
	s.grpcServer.GracefulStop()
}
