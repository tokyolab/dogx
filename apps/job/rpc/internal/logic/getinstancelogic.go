package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetInstanceLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetInstanceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetInstanceLogic {
	return &GetInstanceLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetInstanceLogic) GetInstance(in *job.GetInstanceRequest) (*job.GetInstanceResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidInput()
	}
	instance, err := l.svcCtx.InstanceRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, instanceQueryError("get job instance", err)
	}
	if instance == nil {
		return nil, missingInstanceRecord()
	}
	// JSON null or an empty object decodes into a zero-value model. Do not
	// present invented configuration defaults or reinterpret an unknown schema.
	config := instance.ExecutionConfig
	if config.SchemaVersion != 1 || (config.ExecutionMode != model.ExecutionModeNormal && config.ExecutionMode != model.ExecutionModeSharded) {
		return nil, status.Error(codes.Internal, "stored instance execution configuration is invalid")
	}
	return &job.GetInstanceResponse{Instance: instanceInfo(instance)}, nil
}
