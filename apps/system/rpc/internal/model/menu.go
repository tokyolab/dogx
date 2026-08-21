package model

type MenuType int16

const (
	MenuTypeDirectory MenuType = 1
	MenuTypePage      MenuType = 2
	MenuTypeButton    MenuType = 3
)

type Menu struct {
	Base
	ParentID   *int64       `gorm:"column:parent_id"`
	Type       MenuType     `gorm:"column:menu_type;not null"`
	Name       string       `gorm:"column:name;size:64;not null"`
	RouteName  string       `gorm:"column:route_name;size:128;not null"`
	Path       string       `gorm:"column:path;size:255;not null"`
	Component  string       `gorm:"column:component;size:255;not null"`
	Permission string       `gorm:"column:permission;size:128;not null"`
	Icon       string       `gorm:"column:icon;size:128;not null"`
	Sort       int32        `gorm:"column:sort;not null"`
	Visible    bool         `gorm:"column:visible;not null"`
	Status     RecordStatus `gorm:"column:status;not null"`
	KeepAlive  bool         `gorm:"column:keep_alive;not null"`
	External   bool         `gorm:"column:external;not null"`
	Remark     string       `gorm:"column:remark;size:500;not null"`
}

func (Menu) TableName() string {
	return "sys_menu"
}
