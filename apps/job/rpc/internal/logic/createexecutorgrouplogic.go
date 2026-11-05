package logic

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CreateExecutorGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateExecutorGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateExecutorGroupLogic {
	return &CreateExecutorGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateExecutorGroupLogic) CreateExecutorGroup(in *job.CreateExecutorGroupRequest) (*job.CreateExecutorGroupResponse, error) {
	if in == nil || !validGroupIdentity(in.Name, 128) || !validGroupIdentity(in.DiscoveryKey, 255) {
		return nil, invalidInput()
	}
	// Use the cryptographic UUID source and return its failure rather than
	// producing a fixed credential or panicking during a management request.
	identity, err := uuid.NewRandom()
	if err != nil {
		return nil, rpcOperationError("generate executor group credential", err)
	}
	group := &model.ExecutorGroup{
		Name: in.Name, DiscoveryKey: in.DiscoveryKey,
		Token: strings.ReplaceAll(identity.String(), "-", ""),
	}
	id, err := l.svcCtx.GroupRepo.Create(l.ctx, group)
	if err != nil {
		return nil, executorGroupError("create executor group", err)
	}
	if id <= 0 {
		return nil, status.Error(codes.Internal, "executor group repository returned an invalid identity without an error")
	}
	return &job.CreateExecutorGroupResponse{Id: id}, nil
}
