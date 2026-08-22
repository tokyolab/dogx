package model

import (
	"time"

	"gorm.io/gorm"
)

type RecordStatus int16

const (
	RecordStatusDisabled RecordStatus = 0
	RecordStatusEnabled  RecordStatus = 1
)

type Base struct {
	ID        int64          `gorm:"column:id;primaryKey;autoIncrement"`
	CreatedAt time.Time      `gorm:"column:created_at"`
	UpdatedAt time.Time      `gorm:"column:updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index"`
}
