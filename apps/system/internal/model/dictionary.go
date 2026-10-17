package model

import basemodel "github.com/tokyolab/dogx/pkg/model"

type Dictionary struct {
	basemodel.Base
	Name     string                 `gorm:"column:name"`
	Code     string                 `gorm:"column:code"`
	Remark   string                 `gorm:"column:remark"`
	Status   basemodel.RecordStatus `gorm:"column:status"`
	IsPublic bool                   `gorm:"column:is_public"`
}

func (Dictionary) TableName() string { return "sys_dictionary" }
