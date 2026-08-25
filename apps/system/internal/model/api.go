package model

type API struct {
	Base
	ServiceName string       `gorm:"column:service_name;size:64;not null"`
	Group       string       `gorm:"column:api_group;size:64;not null"`
	Name        string       `gorm:"column:name;size:128;not null"`
	Path        string       `gorm:"column:path;size:100;not null"`
	Method      string       `gorm:"column:method;size:16;not null"`
	IsRequired  bool         `gorm:"column:is_required;not null"`
	Status      RecordStatus `gorm:"column:status;not null"`
	Remark      string       `gorm:"column:remark;size:500;not null"`
}

func (API) TableName() string {
	return "sys_api"
}
