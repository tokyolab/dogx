package model

import basemodel "github.com/tokyolab/dogx/pkg/model"

type ExecutorGroup struct {
	basemodel.Base
	Name string `gorm:"type:varchar(128);not null"`
}

func (ExecutorGroup) TableName() string { return "job_executor_group" }
