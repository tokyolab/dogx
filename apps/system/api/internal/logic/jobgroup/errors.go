package jobgroup

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func invalidRequest() error {
	return status.Error(codes.InvalidArgument, "invalid executor group request")
}

func invalidResponse() error {
	return status.Error(codes.Internal, "job RPC returned an invalid executor group response")
}
