package menu

import (
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toMenuFields(in types.MenuFields) *systemclient.MenuFields {
	return &systemclient.MenuFields{
		ParentId:   in.ParentId,
		Type:       in.Type,
		Name:       in.Name,
		RouteName:  in.RouteName,
		Path:       in.Path,
		Component:  in.Component,
		Permission: in.Permission,
		Icon:       in.Icon,
		Sort:       in.Sort,
		Visible:    in.Visible,
		KeepAlive:  in.KeepAlive,
		External:   in.External,
		Remark:     in.Remark,
	}
}

func toMenuItem(in *systemclient.MenuInfo) (*types.MenuItem, error) {
	if in == nil || in.Menu == nil || in.Id <= 0 {
		return nil, status.Error(codes.Internal, "system RPC returned an invalid menu")
	}
	return &types.MenuItem{
		Id:        in.Id,
		Status:    in.Status,
		CreatedAt: in.CreatedAt,
		UpdatedAt: in.UpdatedAt,
		MenuFields: types.MenuFields{
			ParentId:   in.Menu.ParentId,
			Type:       in.Menu.Type,
			Name:       in.Menu.Name,
			RouteName:  in.Menu.RouteName,
			Path:       in.Menu.Path,
			Component:  in.Menu.Component,
			Permission: in.Menu.Permission,
			Icon:       in.Menu.Icon,
			Sort:       in.Menu.Sort,
			Visible:    in.Menu.Visible,
			KeepAlive:  in.Menu.KeepAlive,
			External:   in.Menu.External,
			Remark:     in.Menu.Remark,
		},
	}, nil
}

func invalidMenuRequest() error { return status.Error(codes.InvalidArgument, "invalid menu request") }
func invalidMenuResponse() error {
	return status.Error(codes.Internal, "system RPC returned an invalid menu response")
}
