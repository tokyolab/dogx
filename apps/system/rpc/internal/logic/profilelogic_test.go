package logic

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetProfile(t *testing.T) {
	email, phone := "a@example.com", "123"
	repo := &userRepositoryStub{record: &repository.UserRecord{User: model.User{Username: "alice", Nickname: "Alice", Email: &email, Phone: &phone, Status: model.RecordStatusEnabled}, DepartmentName: "Sales", Roles: []model.Role{{Name: "Reader"}, {Name: "Disabled role", Status: model.RecordStatusDisabled}}}}
	logic := NewGetProfileLogic(context.Background(), &svc.ServiceContext{UserRepo: repo})
	got, err := logic.GetProfile(&system.CurrentUserRequest{UserId: 42})
	if err != nil || got.Username != "alice" || got.Nickname != "Alice" || got.Email != email || got.Phone != phone || got.DepartmentName != "Sales" || len(got.Roles) != 2 {
		t.Fatalf("profile: %+v %v", got, err)
	}
	repo.record.User.Email, repo.record.User.Phone = nil, nil
	repo.record.Roles = nil
	got, err = logic.GetProfile(&system.CurrentUserRequest{UserId: 42})
	if err != nil || got.Email != "" || got.Phone != "" || len(got.Roles) != 0 {
		t.Fatalf("optional fields: %+v %v", got, err)
	}
	for _, in := range []*system.CurrentUserRequest{nil, {}, {UserId: -1}} {
		if _, err := logic.GetProfile(in); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid input: %v", err)
		}
	}
	repo.record.User.Status = model.RecordStatusDisabled
	if _, err := logic.GetProfile(&system.CurrentUserRequest{UserId: 42}); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	repo.record = nil
	if _, err := logic.GetProfile(&system.CurrentUserRequest{UserId: 42}); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	repo.findErr = errors.New("database unavailable")
	if _, err := logic.GetProfile(&system.CurrentUserRequest{UserId: 42}); !errors.Is(err, repo.findErr) {
		t.Fatal(err)
	}
}

func TestUpdateProfileValidationAndNormalization(t *testing.T) {
	repo := &userRepositoryStub{}
	logic := NewUpdateProfileLogic(context.Background(), &svc.ServiceContext{UserRepo: repo})
	for _, in := range []*system.UpdateProfileRequest{nil, {}, {UserId: -1, Nickname: "Alice"}, {UserId: 42, Nickname: " "}, {UserId: 42, Nickname: strings.Repeat("中", 65)}, {UserId: 42, Nickname: "Alice", Email: "invalid"}, {UserId: 42, Nickname: "Alice", Phone: strings.Repeat("1", 33)}} {
		if _, err := logic.UpdateProfile(in); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid request accepted: %v", err)
		}
	}
	if repo.writeCalls != 0 {
		t.Fatal("invalid input reached database")
	}
	in := &system.UpdateProfileRequest{UserId: 42, Nickname: " Alice ", Email: " a@example.com ", Phone: " 123 "}
	if _, err := logic.UpdateProfile(in); err != nil {
		t.Fatal(err)
	}
	if repo.contactUserID != 42 || repo.contacts.Nickname != "Alice" || *repo.contacts.Email != "a@example.com" || *repo.contacts.Phone != "123" {
		t.Fatalf("normalization: %+v", repo.contacts)
	}
	in.Email, in.Phone = "", " "
	if _, err := logic.UpdateProfile(in); err != nil || repo.contacts.Email != nil || repo.contacts.Phone != nil {
		t.Fatalf("clear optional fields: %v", err)
	}
}

func TestUpdateProfileWriteErrors(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason string
	}{
		{repository.ErrUserEmailExists, subcode.UserEmailExists},
		{repository.ErrUserPhoneExists, subcode.UserPhoneExists},
	} {
		repo := &userRepositoryStub{updateErr: tc.err}
		_, err := NewUpdateProfileLogic(context.Background(), &svc.ServiceContext{UserRepo: repo}).UpdateProfile(&system.UpdateProfileRequest{UserId: 42, Nickname: "Alice"})
		business, ok := bizerror.From(err)
		if !ok || business.Subcode() != tc.reason {
			t.Fatalf("business code: %v", err)
		}
	}
	repo := &userRepositoryStub{updateErr: repository.ErrUserNotFound}
	logic := NewUpdateProfileLogic(context.Background(), &svc.ServiceContext{UserRepo: repo})
	if _, err := logic.UpdateProfile(&system.UpdateProfileRequest{UserId: 42, Nickname: "Alice"}); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	repo.updateErr = errors.New("database unavailable")
	if _, err := logic.UpdateProfile(&system.UpdateProfileRequest{UserId: 42, Nickname: "Alice"}); !errors.Is(err, repo.updateErr) {
		t.Fatal(err)
	}
}
