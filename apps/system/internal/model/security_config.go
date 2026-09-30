package model

import "time"

// SecurityConfig is the singleton configuration; it is not a soft-deletable resource.
type SecurityConfig struct {
	ID                          int64 `gorm:"primaryKey;autoIncrement:false"`
	LoginRateLimitEnabled       bool
	LoginRateLimitWindowSeconds int32
	LoginRateLimitMaxRequests   int32
	LoginFailureLockEnabled     bool
	LoginFailureWindowSeconds   int32
	LoginFailureThreshold       int32
	LoginLockDurationSeconds    int32
	CreatedAt                   time.Time
	UpdatedAt                   time.Time
}

func (SecurityConfig) TableName() string { return "sys_security_config" }
