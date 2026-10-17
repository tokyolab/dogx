package model

import basemodel "github.com/tokyolab/dogx/pkg/model"

// ExecutorGroup stores a group identity and its soft-delete state, not a live
// executor process. It currently holds only the minimum group data referenced
// by Job configuration, without discovery targets or machine credentials.
type ExecutorGroup struct {
	basemodel.Base
	Name string `gorm:"type:varchar(128);not null"`
}

func (ExecutorGroup) TableName() string { return "job_executor_group" }
