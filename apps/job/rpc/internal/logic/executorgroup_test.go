package logic

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestCreateExecutorGroupPreservesIdentityAndGeneratesCredential(t *testing.T) {
	r := &jobRepositoryStub{groupID: 41}
	s := managementContext(r)
	s.NotifyJobChanged = func(context.Context) error { t.Fatal("group creation notified Cron configuration"); return nil }
	in := &job.CreateExecutorGroupRequest{Name: "System.Workers", DiscoveryKey: "/Test//system.rpc/"}
	var previous string
	for range 2 {
		response, err := NewCreateExecutorGroupLogic(context.Background(), s).CreateExecutorGroup(in)
		if err != nil || response.GetId() != 41 || r.groupSaved.Name != in.Name || r.groupSaved.DiscoveryKey != in.DiscoveryKey {
			t.Fatalf("create response=%v error=%v", response, err)
		}
		token := r.groupSaved.Token
		identity, err := uuid.Parse(token)
		if err != nil || len(token) != 32 || token != strings.ToLower(token) || strings.Contains(token, "-") || identity.Version() != 4 || identity.Variant() != uuid.RFC4122 || token == previous {
			t.Fatal("creation did not produce a distinct lowercase UUIDv4 credential")
		}
		previous = token
		if r.groupSaved.ID != 0 || !r.groupSaved.CreatedAt.IsZero() || !r.groupSaved.UpdatedAt.IsZero() {
			t.Fatal("Logic supplied audit fields instead of letting persistence initialize them")
		}
		encoded, err := protojson.Marshal(response)
		if err != nil || strings.Contains(string(encoded), token) || strings.Contains(string(encoded), "token") {
			t.Fatal("creation response exposed credentials")
		}
	}
	if strings.Join(r.calls, ",") != "group_create,group_create" {
		t.Fatalf("unexpected creation operations: %v", r.calls)
	}
}

type failingCredentialSource struct{ err error }

func (s failingCredentialSource) Read([]byte) (int, error) { return 0, s.err }

func TestCreateExecutorGroupStopsWhenSecureRandomSourceFails(t *testing.T) {
	// UUID exposes a process-wide random source for testing. This test must stay
	// non-parallel, and restores the cryptographic default before the next test.
	failure := errors.New("secure random source unavailable")
	uuid.SetRand(failingCredentialSource{err: failure})
	t.Cleanup(func() { uuid.SetRand(nil) })
	r := &jobRepositoryStub{groupID: 41}
	response, err := NewCreateExecutorGroupLogic(context.Background(), managementContext(r)).CreateExecutorGroup(&job.CreateExecutorGroupRequest{Name: "group", DiscoveryKey: "group.rpc"})
	if response != nil || !errors.Is(err, failure) || len(r.calls) != 0 {
		t.Fatalf("credential failure reached persistence or lost cause: calls=%v error=%v", r.calls, err)
	}
}

func TestExecutorGroupCreationRejectsInvalidIdentityWithoutPersistence(t *testing.T) {
	for _, in := range []*job.CreateExecutorGroupRequest{
		nil,
		{Name: "", DiscoveryKey: "valid"}, {Name: "valid", DiscoveryKey: ""},
		{Name: " group", DiscoveryKey: "valid"}, {Name: "group\u3000", DiscoveryKey: "valid"},
		{Name: "valid", DiscoveryKey: " key"}, {Name: "valid", DiscoveryKey: "key\t"},
		{Name: "bad\x00name", DiscoveryKey: "valid"}, {Name: "valid", DiscoveryKey: "bad\x00key"},
		{Name: string([]byte{0xff}), DiscoveryKey: "valid"}, {Name: "valid", DiscoveryKey: string([]byte{0xff})},
		{Name: strings.Repeat("界", 129), DiscoveryKey: "valid"}, {Name: "valid", DiscoveryKey: strings.Repeat("界", 256)},
	} {
		response, err := NewCreateExecutorGroupLogic(context.Background(), &svc.ServiceContext{}).CreateExecutorGroup(in)
		if response != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid identity response=%v error=%v", response, err)
		}
	}
	r := &jobRepositoryStub{groupID: 9}
	response, err := NewCreateExecutorGroupLogic(context.Background(), managementContext(r)).CreateExecutorGroup(&job.CreateExecutorGroupRequest{
		Name: strings.Repeat("界", 128), DiscoveryKey: strings.Repeat("界", 255),
	})
	if err != nil || response.GetId() != 9 {
		t.Fatalf("Unicode character limits rejected: %v", err)
	}
}

func TestExecutorGroupDetailAndListProjections(t *testing.T) {
	at := time.Date(2026, 11, 4, 7, 8, 9, 123, time.FixedZone("test", 8*60*60))
	r := &jobRepositoryStub{
		group:      &model.ExecutorGroup{Base: basemodel.Base{ID: 19, CreatedAt: at, UpdatedAt: at}, Name: "group", DiscoveryKey: "group.rpc", Token: "detail-only-token"},
		groupItems: []repository.ExecutorGroupListItem{{ID: 19, Name: "group", DiscoveryKey: "group.rpc", CreatedAt: at, UpdatedAt: at}},
		total:      3,
	}
	s := managementContext(r)
	detail, err := NewGetExecutorGroupLogic(context.Background(), s).GetExecutorGroup(&job.GetExecutorGroupRequest{Id: 19})
	if err != nil || detail.GetGroup().GetToken() != r.group.Token || detail.Group.Name != r.group.Name || detail.Group.DiscoveryKey != r.group.DiscoveryKey || detail.Group.CreatedAt != at.Format(time.RFC3339Nano) || detail.Group.UpdatedAt != at.Format(time.RFC3339Nano) {
		t.Fatalf("detail projection failed: %v", err)
	}
	list, err := NewListExecutorGroupsLogic(context.Background(), s).ListExecutorGroups(&job.ListExecutorGroupsRequest{Page: 2, PageSize: 1, Name: "group%_"})
	if err != nil || list.Total != 3 || len(list.Items) != 1 || list.Items[0].Id != 19 || list.Items[0].CreatedAt != detail.Group.CreatedAt || list.Items[0].UpdatedAt != detail.Group.UpdatedAt {
		t.Fatalf("list projection failed: %v", err)
	}
	if r.groupFilter != (repository.ExecutorGroupFilter{Page: 2, PageSize: 1, Name: "group%_"}) {
		t.Fatalf("list filter=%+v", r.groupFilter)
	}
	encoded, err := protojson.Marshal(list)
	if err != nil || strings.Contains(string(encoded), r.group.Token) || strings.Contains(string(encoded), "token") {
		t.Fatal("list response exposed credentials")
	}
	r.groupItems = nil
	list, err = NewListExecutorGroupsLogic(context.Background(), s).ListExecutorGroups(&job.ListExecutorGroupsRequest{Page: 1, PageSize: 20})
	if err != nil || list.Items == nil || len(list.Items) != 0 {
		t.Fatalf("empty list must be non-nil: %v", err)
	}
}

func TestExecutorGroupManagementRejectsInvalidIDsAndPagination(t *testing.T) {
	s := &svc.ServiceContext{}
	for _, in := range []*job.GetExecutorGroupRequest{nil, {}, {Id: -1}} {
		response, err := NewGetExecutorGroupLogic(context.Background(), s).GetExecutorGroup(in)
		if response != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid get response=%v error=%v", response, err)
		}
	}
	for _, in := range []*job.DeleteExecutorGroupRequest{nil, {}, {Id: -1}} {
		response, err := NewDeleteExecutorGroupLogic(context.Background(), s).DeleteExecutorGroup(in)
		if response != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid delete response=%v error=%v", response, err)
		}
	}
	for _, in := range []*job.ListExecutorGroupsRequest{
		nil, {}, {Page: 0, PageSize: 20}, {Page: 1, PageSize: 201}, {Page: math.MaxInt64, PageSize: 200},
		{Page: 1, PageSize: 20, Name: "bad\x00name"}, {Page: 1, PageSize: 20, Name: string([]byte{0xff})},
		{Page: 1, PageSize: 20, Name: strings.Repeat("界", 129)},
	} {
		response, err := NewListExecutorGroupsLogic(context.Background(), s).ListExecutorGroups(in)
		if response != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid list response=%v error=%v", response, err)
		}
	}
}

func TestExecutorGroupErrorMappingAndMissingRecords(t *testing.T) {
	for _, test := range []struct {
		err  error
		code string
	}{
		{repository.ErrExecutorGroupUnavailable, subcode.ExecutorGroupNotFound},
		{repository.ErrExecutorGroupNameExists, subcode.ExecutorGroupNameExists},
		{repository.ErrExecutorGroupDiscoveryKeyExists, subcode.ExecutorGroupDiscoveryKeyExists},
	} {
		r := &jobRepositoryStub{err: test.err}
		_, err := NewCreateExecutorGroupLogic(context.Background(), managementContext(r)).CreateExecutorGroup(&job.CreateExecutorGroupRequest{Name: "group", DiscoveryKey: "group.rpc"})
		requireSubcode(t, err, test.code)
	}
	r := &jobRepositoryStub{err: repository.ErrExecutorGroupUnavailable}
	_, err := NewGetExecutorGroupLogic(context.Background(), managementContext(r)).GetExecutorGroup(&job.GetExecutorGroupRequest{Id: 1})
	requireSubcode(t, err, subcode.ExecutorGroupNotFound)
	// Missing groups in Job configuration keep their original public subcode.
	requireSubcode(t, jobManagementError("configure job", repository.ErrExecutorGroupUnavailable), subcode.JobExecutorGroupUnavailable)
	r.err = nil
	_, err = NewGetExecutorGroupLogic(context.Background(), managementContext(r)).GetExecutorGroup(&job.GetExecutorGroupRequest{Id: 1})
	if status.Code(err) != codes.Internal {
		t.Fatalf("nil detail was not an internal failure: %v", err)
	}
	_, err = NewCreateExecutorGroupLogic(context.Background(), managementContext(r)).CreateExecutorGroup(&job.CreateExecutorGroupRequest{Name: "group", DiscoveryKey: "group.rpc"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("invalid new identity was not an internal failure: %v", err)
	}
}

func TestDeleteExecutorGroupChecksReferencesInTransactionOrder(t *testing.T) {
	for _, test := range []struct {
		name                 string
		jobReference, active bool
		wantCalls            string
	}{
		{"referenced Job", true, false, "group_lock,group_references"},
		{"active old round", false, true, "group_lock,group_references,group_active"},
		{"unused group", false, false, "group_lock,group_references,group_active,group_delete"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &jobRepositoryStub{available: true, groupReferences: test.jobReference, groupActive: test.active}
			s := managementContext(r)
			s.NotifyJobChanged = func(context.Context) error { t.Fatal("group deletion notified Cron configuration"); return nil }
			response, err := NewDeleteExecutorGroupLogic(context.Background(), s).DeleteExecutorGroup(&job.DeleteExecutorGroupRequest{Id: 3})
			if test.jobReference || test.active {
				requireSubcode(t, err, subcode.ExecutorGroupInUse)
				if response != nil {
					t.Fatal("rejected deletion returned a success response")
				}
			} else if err != nil || response == nil {
				t.Fatalf("unused group deletion failed: %v", err)
			}
			if strings.Join(r.calls, ",") != test.wantCalls {
				t.Fatalf("deletion calls=%v want=%s", r.calls, test.wantCalls)
			}
		})
	}
}

func TestExecutorGroupTechnicalFailuresPreserveCauseAndNeverSucceed(t *testing.T) {
	failure := errors.New("database result is unconfirmed")
	for _, step := range []string{"group_lock", "group_references", "group_active", "group_delete", "commit"} {
		t.Run(step, func(t *testing.T) {
			r := &jobRepositoryStub{available: true, failures: map[string]error{step: failure}}
			s := managementContext(r)
			if step == "commit" {
				s.Transactions = func(_ context.Context, fn func(repository.Repositories) error) error {
					if err := fn(repository.Repositories{Jobs: r, Groups: &groupRepositoryStub{r}, Instances: &managementInstanceStub{r}}); err != nil {
						return err
					}
					return failure
				}
			}
			response, err := NewDeleteExecutorGroupLogic(context.Background(), s).DeleteExecutorGroup(&job.DeleteExecutorGroupRequest{Id: 3})
			if response != nil || !errors.Is(err, failure) {
				t.Fatalf("delete claimed success or lost cause: response=%v error=%v", response, err)
			}
		})
	}
	for _, failure := range []error{failure, context.Canceled, context.DeadlineExceeded} {
		r := &jobRepositoryStub{err: failure}
		s := managementContext(r)
		_, createErr := NewCreateExecutorGroupLogic(context.Background(), s).CreateExecutorGroup(&job.CreateExecutorGroupRequest{Name: "group", DiscoveryKey: "group.rpc"})
		_, getErr := NewGetExecutorGroupLogic(context.Background(), s).GetExecutorGroup(&job.GetExecutorGroupRequest{Id: 3})
		_, listErr := NewListExecutorGroupsLogic(context.Background(), s).ListExecutorGroups(&job.ListExecutorGroupsRequest{Page: 1, PageSize: 20})
		for _, err := range []error{createErr, getErr, listErr} {
			if !errors.Is(err, failure) {
				t.Fatalf("technical error lost cause: %v", err)
			}
		}
	}
}
