package bizerror

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type errorHealthServer struct {
	healthpb.UnimplementedHealthServer
}

func (errorHealthServer) Check(
	_ context.Context,
	in *healthpb.HealthCheckRequest,
) (*healthpb.HealthCheckResponse, error) {
	switch in.Service {
	case "success":
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	case "business":
		return nil, New("username already exists")
	case "invalid-business":
		return nil, NewCode(1, "invalid business code")
	case "standard":
		return nil, status.Error(codes.FailedPrecondition, "standard precondition failure")
	default:
		return nil, errors.New("ordinary server failure")
	}
}

func TestUnaryServerInterceptorAcrossGRPC(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor))
	healthpb.RegisterHealthServer(server, errorHealthServer{})

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-serveErr
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create gRPC client: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	client := healthpb.NewHealthClient(conn)

	response, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "success"})
	if err != nil {
		t.Fatalf("successful call returned error: %v", err)
	}
	if response.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("unexpected successful response: %+v", response)
	}

	_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "business"})
	if got := uint32(status.Code(err)); got != DefaultCode {
		t.Fatalf("business code = %d, want %d", got, DefaultCode)
	}
	if got := status.Convert(err).Message(); got != "username already exists" {
		t.Fatalf("unexpected business message: %s", got)
	}

	_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "invalid-business"})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("invalid business code mapped to %s, want %s", got, codes.Internal)
	}
	if got := status.Convert(err).Message(); got != "internal server error" {
		t.Fatalf("unexpected invalid-code message: %s", got)
	}

	_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "standard"})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("standard code = %d, want %d", got, codes.FailedPrecondition)
	}

	_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "ordinary"})
	if got := status.Code(err); got != codes.Unknown {
		t.Fatalf("ordinary code = %d, want %d", got, codes.Unknown)
	}
}
