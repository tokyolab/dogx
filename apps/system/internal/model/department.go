package model

type Department struct {
	Base
	ParentID *int64       `gorm:"column:parent_id"`
	Name     string       `gorm:"column:name;size:128;not null"`
	Sort     int32        `gorm:"column:sort;not null"`
	Status   RecordStatus `gorm:"column:status;not null"`
	Remark   string       `gorm:"column:remark;size:500;not null"`
}

func (Department) TableName() string {
	return "sys_department"
}
