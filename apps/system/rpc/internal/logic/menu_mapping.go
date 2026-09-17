package logic

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var menuRouteNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
var menuComponentPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$`)

func invalidMenuRequest() error { return status.Error(codes.InvalidArgument, "invalid menu request") }

func normalizeMenuInput(in *system.MenuFields) (*model.Menu, error) {
	if in == nil || in.ParentId < 0 || in.Type < 1 || in.Type > 3 || in.Sort < 0 {
		return nil, invalidMenuRequest()
	}
	// Validate enum ranges before narrowing to SMALLINT-backed model types.
	menu := &model.Menu{
		AppCode:    model.MenuAppAdminWeb,
		Type:       model.MenuType(in.Type),
		Name:       strings.TrimSpace(in.Name),
		RouteName:  strings.TrimSpace(in.RouteName),
		Path:       strings.TrimSpace(in.Path),
		Component:  strings.TrimSpace(in.Component),
		Permission: strings.TrimSpace(in.Permission),
		Icon:       strings.TrimSpace(in.Icon),
		Sort:       in.Sort,
		Visible:    in.Visible,
		KeepAlive:  in.KeepAlive,
		External:   in.External,
		Remark:     strings.TrimSpace(in.Remark),
	}
	if in.ParentId > 0 {
		id := in.ParentId
		menu.ParentID = &id
	}
	for _, field := range []struct {
		value string
		limit int
	}{
		{menu.Name, 64},
		{menu.RouteName, 128},
		{menu.Path, 255},
		{menu.Component, 255},
		{menu.Permission, 128},
		{menu.Icon, 128},
		{menu.Remark, 500},
	} {
		if !utf8.ValidString(field.value) || strings.ContainsRune(field.value, 0) || utf8.RuneCountInString(field.value) > field.limit {
			return nil, invalidMenuRequest()
		}
	}
	if menu.Name == "" {
		return nil, invalidMenuRequest()
	}
	if menu.Type == model.MenuTypeElement {
		if menu.Permission == "" || strings.IndexFunc(menu.Permission, unicode.IsSpace) >= 0 {
			return nil, invalidMenuRequest()
		}
		// Type changes must not leave an element with a routable URL or page flags.
		menu.RouteName, menu.Path, menu.Component, menu.Icon = "", "", "", ""
		menu.Visible, menu.KeepAlive, menu.External = false, false, false
		return menu, nil
	}
	menu.Permission = ""
	if !menuRouteNamePattern.MatchString(menu.RouteName) {
		return nil, invalidMenuRequest()
	}
	if menu.Type == model.MenuTypeDirectory {
		menu.Component, menu.KeepAlive, menu.External = "", false, false
	}
	if menu.External {
		parsed, err := url.Parse(menu.Path)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || strings.IndexFunc(menu.Path, unicode.IsSpace) >= 0 {
			return nil, invalidMenuRequest()
		}
		menu.Component, menu.KeepAlive = "", false
	} else {
		if !strings.HasPrefix(menu.Path, "/") || strings.HasPrefix(menu.Path, "//") || strings.ContainsAny(menu.Path, "?#\\") || strings.IndexFunc(menu.Path, unicode.IsSpace) >= 0 {
			return nil, invalidMenuRequest()
		}
		if menu.Type == model.MenuTypePage && !menuComponentPattern.MatchString(menu.Component) {
			return nil, invalidMenuRequest()
		}
	}
	return menu, nil
}

func toMenuInfo(menu model.Menu) *system.MenuInfo {
	var parentID int64
	if menu.ParentID != nil {
		parentID = *menu.ParentID
	}
	return &system.MenuInfo{
		Id:        menu.ID,
		Status:    int32(menu.Status),
		CreatedAt: menu.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: menu.UpdatedAt.UTC().Format(time.RFC3339Nano),
		Menu: &system.MenuFields{
			ParentId:   parentID,
			Type:       int32(menu.Type),
			Name:       menu.Name,
			RouteName:  menu.RouteName,
			Path:       menu.Path,
			Component:  menu.Component,
			Permission: menu.Permission,
			Icon:       menu.Icon,
			Sort:       menu.Sort,
			Visible:    menu.Visible,
			KeepAlive:  menu.KeepAlive,
			External:   menu.External,
			Remark:     menu.Remark,
		},
	}
}

func menuBusinessError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, repository.ErrMenuNotFound):
		return bizerror.New(subcode.MenuNotFound, "菜单不存在")
	case errors.Is(err, repository.ErrMenuParentInvalid):
		return bizerror.New(subcode.MenuParentInvalid, "上级节点不存在或不能作为父节点")
	case errors.Is(err, repository.ErrMenuCycle):
		return bizerror.New(subcode.MenuCycle, "不能选择自己或下级节点作为上级")
	case errors.Is(err, repository.ErrMenuHasChildren):
		return bizerror.New(subcode.MenuHasChildren, "请先调整下级节点，再修改类型或删除")
	case errors.Is(err, repository.ErrMenuRouteNameExists):
		return bizerror.New(subcode.MenuRouteNameExists, "路由名称已存在")
	case errors.Is(err, repository.ErrMenuPathExists):
		return bizerror.New(subcode.MenuPathExists, "路由路径已存在")
	default:
		return fmt.Errorf("menu operation: %w", err)
	}
}
