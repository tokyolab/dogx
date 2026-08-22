package model

import "time"

type User struct {
	Base
	Username     string       `gorm:"column:username;size:64;not null"`
	PasswordHash string       `gorm:"column:password_hash;size:255;not null"`
	Nickname     string       `gorm:"column:nickname;size:64;not null"`
	Email        *string      `gorm:"column:email;size:255"`
	Phone        *string      `gorm:"column:phone;size:32"`
	Status       RecordStatus `gorm:"column:status;not null"`
	LastLoginAt  *time.Time   `gorm:"column:last_login_at"`
	Remark       string       `gorm:"column:remark;size:500;not null"`
}

func (User) TableName() string {
	return "sys_user"
}
