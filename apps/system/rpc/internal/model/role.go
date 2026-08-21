package model

type Role struct {
	Base
	Code        string       `gorm:"column:code;size:64;not null"`
	Name        string       `gorm:"column:name;size:64;not null"`
	Description string       `gorm:"column:description;size:500;not null"`
	Sort        int32        `gorm:"column:sort;not null"`
	Status      RecordStatus `gorm:"column:status;not null"`
}

func (Role) TableName() string {
	return "sys_role"
}
