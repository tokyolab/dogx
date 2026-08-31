package bizerror

import (
	"context"

	"github.com/zeromicro/go-zero/core/logc"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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
	if !IsSubcode(bizErr.Subcode()) {
		logc.Errorf(ctx, "invalid business error subcode: %q", bizErr.Subcode())
		return nil, status.Error(codes.Internal, "internal server error")
	}

	st, detailErr := status.New(codes.Code(bizErr.Code()), bizErr.Error()).WithDetails(
		&errdetails.ErrorInfo{Reason: bizErr.Subcode()},
	)
	if detailErr != nil {
		logc.Errorf(ctx, "attach business error detail: %v", detailErr)
		return nil, status.Error(codes.Internal, "internal server error")
	}
	return nil, st.Err()
}
