package security

import (
	"context"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
)

type systemRPCStub struct {
	systemclient.System
	cfg, update *systemclient.LoginSecurityConfig
	err         error
}

func (s *systemRPCStub) GetLoginSecurity(context.Context, *systemclient.GetLoginSecurityRequest, ...grpc.CallOption) (*systemclient.LoginSecurityConfig, error) {
	return s.cfg, s.err
}
func (s *systemRPCStub) UpdateLoginSecurity(_ context.Context, in *systemclient.LoginSecurityConfig, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.update = in
	return &systemclient.EmptyResponse{}, s.err
}
