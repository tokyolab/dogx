// Package rpchost coordinates zrpc transport, actual unary handlers and process
// shutdown. Server provides an inbound completion barrier; Run adds shared signal,
// deadline and logging coordination around application-owned work and resources.
package rpchost

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server adds an explicit, context-bounded shutdown barrier to zrpc. It must be
// started at most once and cannot be restarted after shutdown. Configuration and
// interceptors must be installed before Start or Shutdown.
//
// Shutdown waits for actual unary handler returns, including handlers that remain
// active after zrpc's timeout interceptor has returned to the client. Background
// work started by a handler is owned and drained separately by the application.
type Server struct {
	rpc *zrpc.RpcServer

	mu            sync.Mutex
	started       bool
	stopping      bool
	grpc          *grpc.Server
	active        int
	registered    chan struct{}
	startDone     chan struct{}
	transportDone chan struct{}
	handlersDone  chan struct{}
	handlersOnce  sync.Once
	gracefulOnce  sync.Once
	forceOnce     sync.Once
}

// NewServer creates a zrpc server. Register may only install services on the
// supplied gRPC server; it must not start or stop that server. All servers in a
// process must receive the same host-owned ServiceConf because zrpc initializes
// process-wide services during construction. A nil registration function is invalid.
func NewServer(c zrpc.RpcServerConf, register func(*grpc.Server)) (*Server, error) {
	if register == nil {
		return nil, errors.New("RPC service registration is required")
	}
	s := &Server{
		registered:    make(chan struct{}),
		startDone:     make(chan struct{}),
		transportDone: make(chan struct{}),
		handlersDone:  make(chan struct{}),
	}
	rpc, err := zrpc.NewServer(c, func(g *grpc.Server) {
		register(g)
		s.mu.Lock()
		if s.stopping {
			// zrpc invokes registration before Serve. Stopping this unserved
			// server synchronously prevents a late startup from accepting work.
			s.forceOnce.Do(g.Stop)
			s.gracefulOnce.Do(func() { close(s.transportDone) })
		}
		s.grpc = g
		close(s.registered)
		s.mu.Unlock()
	})
	if err != nil {
		return nil, fmt.Errorf("create RPC server: %w", err)
	}
	s.rpc = rpc
	// zrpc installs its timeout interceptor during NewServer. Appending here
	// counts the actual inner call, not just the timeout wrapper's lifetime.
	s.rpc.AddUnaryInterceptors(s.trackUnary)
	return s, nil
}

// AddOptions installs gRPC options before Start or Shutdown. Calling it after
// either operation has begun panics, as does changing a running server's setup.
func (s *Server) AddOptions(options ...grpc.ServerOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireConfigurable()
	s.rpc.AddOptions(options...)
}

// AddUnaryInterceptors installs interceptors inside the unary activity barrier.
// Each interceptor must wait for any handler call it starts to return. This
// method must be called before Start or Shutdown and panics otherwise.
func (s *Server) AddUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireConfigurable()
	s.rpc.AddUnaryInterceptors(interceptors...)
}

// Start runs the server once and is suitable for service.WithStarter. If Shutdown
// already began, Start returns without listening. Unexpected startup panics are
// propagated. On Unix, zrpc can keep Start waiting for process-wide shutdown
// listeners even after this server has stopped; a shutdown listener must never
// wait for Start to return.
func (s *Server) Start() {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		panic("rpchost: Start called more than once")
	}
	s.started = true
	stopping := s.stopping
	s.mu.Unlock()
	defer func() {
		close(s.startDone)
		if recovered := recover(); recovered != nil {
			s.mu.Lock()
			expected := s.stopping && s.grpc != nil
			s.mu.Unlock()
			if err, ok := recovered.(error); ok && expected && errors.Is(err, grpc.ErrServerStopped) {
				return
			}
			panic(recovered)
		}
	}()
	if !stopping {
		s.rpc.Start()
	}
}

// Shutdown permanently rejects new unary calls and starts graceful transport
// shutdown. It returns nil only after the transport and actual unary handlers
// have finished, or when Start has never begun. It does not wait for Start's
// process-wide listener barrier and never closes global logging.
//
// Calls may run concurrently. Each caller supplies its own non-nil Context;
// expiration requests at most one asynchronous force stop and returns ctx.Err.
// An expired deadline is not evidence that a handler, registration callback, or
// background work has exited. Later registration is also stopped before Serve.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.stopAdmissionLocked()
	if !s.started {
		s.gracefulOnce.Do(func() { close(s.transportDone) })
		s.mu.Unlock()
		return nil
	}
	g := s.grpc
	s.mu.Unlock()
	if g == nil {
		select {
		case <-s.registered:
		case <-s.startDone:
		case <-ctx.Done():
			s.forceStop()
			return ctx.Err()
		}
		s.mu.Lock()
		g = s.grpc
		s.mu.Unlock()
		if g == nil {
			return nil
		}
	}
	s.gracefulOnce.Do(func() {
		go func() {
			g.GracefulStop()
			close(s.transportDone)
		}()
	})
	if err := s.wait(ctx, s.transportDone); err != nil {
		return err
	}
	return s.wait(ctx, s.handlersDone)
}

// stopAdmission closes the inner unary gate without waiting for active calls.
// The process host uses it at wrap-up, before transport draining begins.
func (s *Server) stopAdmission() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopAdmissionLocked()
}

func (s *Server) stopAdmissionLocked() {
	s.stopping = true
	if s.active == 0 {
		s.handlersOnce.Do(func() { close(s.handlersDone) })
	}
}

func (s *Server) wait(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.forceStop()
		return ctx.Err()
	}
}

func (s *Server) forceStop() {
	s.mu.Lock()
	g := s.grpc
	s.mu.Unlock()
	if g != nil {
		// grpc.Stop can itself wait behind GracefulStop when a handler ignores
		// cancellation. Never let that wait extend the caller's deadline.
		s.forceOnce.Do(func() { go g.Stop() })
	}
}

func (s *Server) requireConfigurable() {
	if s.started || s.stopping {
		panic("rpchost: server configuration is already fixed")
	}
}

func (s *Server) trackUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "RPC server is shutting down")
	}
	s.active++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.active--
		if s.stopping && s.active == 0 {
			s.handlersOnce.Do(func() { close(s.handlersDone) })
		}
	}()
	return handler(ctx, req)
}
