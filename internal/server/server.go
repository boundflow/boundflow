package server

import (
	"fmt"
	"log"
	"net"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	config "github.com/boundflow/boundflow/internal/config"
	"github.com/boundflow/boundflow/internal/auth"
	"github.com/boundflow/boundflow/internal/server/handlers"
	"github.com/boundflow/boundflow/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type WorkerServer struct {
	grpcServer *grpc.Server
	cfg        *config.WorkerConfig
}

func NewWorkerServer(cfg *config.WorkerConfig, workerSvc boundflowv1.WorkerServiceServer, authn *auth.Authenticator) *WorkerServer {
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(authn.UnaryInterceptor()),
		grpc.StreamInterceptor(authn.StreamInterceptor()),
	)
	boundflowv1.RegisterWorkerServiceServer(grpcServer, workerSvc)
	return &WorkerServer{grpcServer: grpcServer, cfg: cfg}
}

func (w *WorkerServer) Start() error {
	addr := fmt.Sprintf(":%d", w.cfg.WorkerGRPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	log.Printf("boundflow worker gRPC server listening on %s", addr)
	return w.grpcServer.Serve(lis)
}

func (w *WorkerServer) Stop() {
	log.Println("shutting down worker gRPC server...")
	w.grpcServer.GracefulStop()
}

type Server struct {
	grpcServer *grpc.Server
	cfg        *config.ServerConfig
}

func New(cfg *config.ServerConfig, regSvc *service.RegistrationService, lifecycleSvc *service.LifecycleService, authn *auth.Authenticator, debug bool) *Server {
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(authn.UnaryInterceptor()),
		grpc.StreamInterceptor(authn.StreamInterceptor()),
	)

	boundflowv1.RegisterRegistrationServiceServer(grpcServer, handlers.NewRegistrationHandler(regSvc))
	boundflowv1.RegisterResourceLifecycleServiceServer(grpcServer, handlers.NewResourceLifecycleHandler(lifecycleSvc))

	if debug {
		reflection.Register(grpcServer)
	}

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

	log.Printf("boundflow gRPC server listening on %s", addr)
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	log.Println("shutting down gRPC server...")
	s.grpcServer.GracefulStop()
}
