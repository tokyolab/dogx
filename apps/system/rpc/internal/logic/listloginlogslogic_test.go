package logic

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListLoginLogs(t *testing.T) {
	for _, result := range []string{"", "success", "failure"} {
		t.Run("filter_"+result, func(t *testing.T) {
			repo := &loginLogRepositoryStub{items: []model.LoginLog{{ID: 3, Username: "alice", Success: false, FailureReason: "invalid_credentials", IPAddress: "192.0.2.1", UserAgent: "Test browser", CreatedAt: time.Date(2026, 9, 29, 12, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))}}, total: 8}
			logic := NewListLoginLogsLogic(context.Background(), &svc.ServiceContext{LoginLogRepo: repo})
			resp, err := logic.ListLoginLogs(&system.ListLoginLogsRequest{Page: 2, PageSize: 2, Username: " alice ", Result: result})
			if err != nil {
				t.Fatal(err)
			}
			if repo.query.Username != "alice" || repo.query.Offset != 2 || repo.query.Limit != 2 || repo.calls != 1 {
				t.Fatalf("query: %+v", repo.query)
			}
			if result == "" {
				if repo.query.Success != nil {
					t.Fatal("unexpected filter")
				}
			} else if repo.query.Success == nil || *repo.query.Success != (result == "success") {
				t.Fatalf("incorrect %s filter", result)
			}
			if resp.Total != 8 || len(resp.Items) != 1 {
				t.Fatalf("response: %+v", resp)
			}
			item := resp.Items[0]
			if item.Id != 3 || item.Username != "alice" || item.Success || item.FailureReason != "invalid_credentials" || item.IpAddress != "192.0.2.1" || item.UserAgent != "Test browser" || item.CreatedAt != "2026-09-29T04:00:00Z" {
				t.Fatalf("item: %+v", item)
			}
		})
	}
}

func TestListLoginLogsRejectsInvalidInput(t *testing.T) {
	cases := []*system.ListLoginLogsRequest{
		nil, {}, {Page: -1, PageSize: 20}, {Page: 1}, {Page: 1, PageSize: -1},
		{Page: 1, PageSize: 201}, {Page: math.MaxInt64, PageSize: 200},
		{Page: 1, PageSize: 20, Username: strings.Repeat("用", 65)},
		{Page: 1, PageSize: 20, Result: "unknown"},
	}
	repo := &loginLogRepositoryStub{}
	logic := NewListLoginLogsLogic(context.Background(), &svc.ServiceContext{LoginLogRepo: repo})
	for _, req := range cases {
		if _, err := logic.ListLoginLogs(req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("input %+v: %v", req, err)
		}
	}
	if repo.calls != 0 {
		t.Fatal("invalid request reached repository")
	}
}

func TestListLoginLogsDependenciesAndEmpty(t *testing.T) {
	req := &system.ListLoginLogsRequest{Page: 1, PageSize: 20}
	if _, err := NewListLoginLogsLogic(context.Background(), &svc.ServiceContext{}).ListLoginLogs(req); err == nil {
		t.Fatal("missing repository accepted")
	}
	failure := errors.New("database unavailable")
	repo := &loginLogRepositoryStub{err: failure}
	logic := NewListLoginLogsLogic(context.Background(), &svc.ServiceContext{LoginLogRepo: repo})
	if _, err := logic.ListLoginLogs(req); !errors.Is(err, failure) {
		t.Fatalf("error: %v", err)
	}
	repo.err = nil
	resp, err := logic.ListLoginLogs(req)
	if err != nil || resp.Items == nil || len(resp.Items) != 0 || resp.Total != 0 {
		t.Fatalf("empty result: %+v %v", resp, err)
	}
}
