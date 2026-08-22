package bizerror

import (
	"context"

	"github.com/zeromicro/go-zero/core/logc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func UnaryServerInterceptor(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	resp, err := handler(ctx, req)
	if err == nil {
		return resp, nil
	}

	bizErr, ok := From(err)
	if !ok {
		return resp, err
	}

	if !IsCode(bizErr.Code()) {
		logc.Errorf(ctx, "invalid business error code: %d", bizErr.Code())
		return nil, status.Error(codes.Internal, "internal server error")
	}

	return nil, status.Error(codes.Code(bizErr.Code()), bizErr.Error())
}
