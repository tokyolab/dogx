package model

import (
	"time"

	"gorm.io/gorm"
)

// Base shares a primary key and technical audit timestamps across soft-deletable
// entities. Business deadlines, versions, and configuration changes need their
// own fields rather than deriving their meaning from these timestamps.
// GORM soft deletion sets DeletedAt without updating UpdatedAt.
type Base struct {
	ID        int64          `gorm:"column:id;primaryKey;autoIncrement"`
	CreatedAt time.Time      `gorm:"column:created_at"`
	UpdatedAt time.Time      `gorm:"column:updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at"`
}
