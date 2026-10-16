package model

import basemodel "github.com/tokyolab/dogx/pkg/model"

type DictionaryItem struct {
	basemodel.Base
	DictionaryID int64        `gorm:"column:dictionary_id"`
	Label        string       `gorm:"column:label"`
	Value        string       `gorm:"column:value"`
	Sort         int32        `gorm:"column:sort"`
	Remark       string       `gorm:"column:remark"`
	Status       RecordStatus `gorm:"column:status"`
}

func (DictionaryItem) TableName() string { return "sys_dictionary_item" }
