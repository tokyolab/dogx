package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type protocolJobsStub struct{ svc.JobService }

func (*protocolJobsStub) GetJob(_ context.Context, id int64) (*repository.JobDetail, error) {
	switch id {
	case 2:
		return nil, service.ErrNotFound
	case 3:
		return nil, context.DeadlineExceeded
	case 4:
		return nil, errors.New("postgres secret.internal offline")
	}
	return &repository.JobDetail{JobDefinition: model.JobDefinition{Base: basemodel.Base{ID: id}}}, nil
}

func TestJobRPCWireBusinessAndTechnicalErrors(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(bizerror.UnaryServerInterceptor))
	job.RegisterJobServer(server, NewJobServer(&svc.ServiceContext{Jobs: &protocolJobsStub{}}))
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	connection, err := grpc.NewClient("passthrough:///job-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := job.NewJobClient(connection)
	for _, test := range []struct {
		id   int64
		code codes.Code
	}{{0, codes.InvalidArgument}, {1, codes.OK}, {2, codes.Code(bizerror.DefaultCode)}, {3, codes.DeadlineExceeded}, {4, codes.Unknown}} {
		t.Run(test.code.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := client.GetJob(ctx, &job.GetJobRequest{Id: test.id})
			if status.Code(err) != test.code {
				t.Fatalf("error=%v want=%s", err, test.code)
			}
			if test.code == codes.OK && (result == nil || result.Job == nil || result.Job.Id != 1) {
				t.Fatalf("result=%+v", result)
			}
			if test.id == 2 {
				st := status.Convert(err)
				reason, ok := bizerror.SubcodeFromStatus(st)
				if !ok || reason != service.SubcodeNotFound || st.Message() != "Job does not exist" {
					t.Fatalf("business status=%v", st)
				}
				for _, detail := range st.Details() {
					if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain != "" {
						t.Fatalf("unexpected error domain=%q", info.Domain)
					}
				}
			}
		})
	}
}
