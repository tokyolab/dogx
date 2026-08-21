package model

import "time"

type UserRole struct {
	UserID    int64     `gorm:"column:user_id;primaryKey"`
	RoleID    int64     `gorm:"column:role_id;primaryKey"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (UserRole) TableName() string {
	return "sys_user_role"
}

type RoleMenu struct {
	RoleID    int64     `gorm:"column:role_id;primaryKey"`
	MenuID    int64     `gorm:"column:menu_id;primaryKey"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (RoleMenu) TableName() string {
	return "sys_role_menu"
}
