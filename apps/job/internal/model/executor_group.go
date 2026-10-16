package model

import "time"

type ExecutorGroup struct {
	ID        int64  `gorm:"primaryKey;autoIncrement"`
	Name      string `gorm:"type:varchar(128);not null"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

func (ExecutorGroup) TableName() string { return "job_executor_group" }
