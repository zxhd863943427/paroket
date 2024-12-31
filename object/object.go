package object

import (
	"database/sql/driver"

	"github.com/google/uuid"
)

type ObjectId uuid.UUID

type Object struct {
	ObjectId ObjectId
}

// Scan 实现 sql.Scanner 接口
func (id *ObjectId) Scan(value interface{}) error {
	return (*uuid.UUID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id ObjectId) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}
