package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/convergeplane/convergeplane/internal/storage"
)

const apiKeyHeader = "x-api-key"

// Authenticator resolves an API key to a tenant group.
type Authenticator struct {
	keys storage.ApiKeyRepository
}

func NewAuthenticator(keys storage.ApiKeyRepository) *Authenticator {
	return &Authenticator{keys: keys}
}

// HashKey returns the hex-encoded SHA-256 of the raw key.
// Used both when provisioning a key and when verifying a request.
func HashKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func (a *Authenticator) resolve(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get(apiKeyHeader)
	if len(vals) == 0 || vals[0] == "" {
		return nil, status.Errorf(codes.Unauthenticated, "missing %s header", apiKeyHeader)
	}

	key, err := a.keys.GetByKeyHash(ctx, HashKey(vals[0]))
	if err != nil {
		// Don't leak whether the key exists or is revoked.
		return nil, status.Error(codes.Unauthenticated, "invalid api key")
	}

	return WithTenantGroup(ctx, key.TenantGroupID), nil
}

// UnaryInterceptor authenticates every unary RPC.
func (a *Authenticator) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := a.resolve(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor authenticates every streaming RPC (e.g. WorkerSession).
func (a *Authenticator) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := a.resolve(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
	}
}

// wrappedStream swaps in the authenticated context for a streaming RPC.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
