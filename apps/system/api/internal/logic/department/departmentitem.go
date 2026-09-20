package department

import (
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ToDepartmentItem(item *systemclient.DepartmentInfo) (types.DepartmentItem, error) {
	if item == nil || item.GetId() <= 0 {
		return types.DepartmentItem{}, status.Error(codes.Internal, "system RPC returned an invalid department")
	}
	return types.DepartmentItem{Id: item.GetId(), ParentId: item.GetParentId(), Name: item.GetName(), Sort: item.GetSort(), Status: item.GetStatus(), Remark: item.GetRemark(), CreatedAt: item.GetCreatedAt(), UpdatedAt: item.GetUpdatedAt()}, nil
}

func invalidDepartmentRequest() error {
	return status.Error(codes.InvalidArgument, "invalid department request")
}
