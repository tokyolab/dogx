package model

import "time"

const (
	LoginFailureInvalidCredentials = "invalid_credentials"
	LoginFailureAccountDisabled    = "account_disabled"
	LoginFailureSystemError        = "system_error"
)

type LoginLog struct {
	ID            int64     `gorm:"column:id;primaryKey;autoIncrement"`
	UserID        *int64    `gorm:"column:user_id"`
	Username      string    `gorm:"column:username;size:64;not null"`
	Success       bool      `gorm:"column:success;not null"`
	FailureReason string    `gorm:"column:failure_reason;size:64;not null"`
	IPAddress     string    `gorm:"column:ip_address;size:45;not null"`
	UserAgent     string    `gorm:"column:user_agent;size:512;not null"`
	CreatedAt     time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (LoginLog) TableName() string {
	return "sys_login_log"
}
