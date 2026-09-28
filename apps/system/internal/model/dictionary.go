package model

type Dictionary struct {
	Base
	Name     string       `gorm:"column:name"`
	Code     string       `gorm:"column:code"`
	Remark   string       `gorm:"column:remark"`
	Status   RecordStatus `gorm:"column:status"`
	IsPublic bool         `gorm:"column:is_public"`
}

func (Dictionary) TableName() string { return "sys_dictionary" }
