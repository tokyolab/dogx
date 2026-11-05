package model

import basemodel "github.com/tokyolab/dogx/pkg/model"

// ExecutorGroup stores immutable execution credentials and discovery metadata,
// not a live executor process. Name and DiscoveryKey remain reserved after soft
// deletion. Token is a plaintext credential and must never be logged.
type ExecutorGroup struct {
	basemodel.Base
	Name         string `gorm:"type:varchar(128);not null"`
	DiscoveryKey string `gorm:"type:varchar(255);not null"`
	Token        string `gorm:"type:varchar(32);not null"`
}

func (ExecutorGroup) TableName() string { return "job_executor_group" }
