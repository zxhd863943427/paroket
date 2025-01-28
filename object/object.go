package object

import (
	"database/sql/driver"

	"github.com/google/uuid"
)

type ObjectId uuid.UUID

type Object struct {
	ObjectId ObjectId
}

func NewObjectId() (ObjectId, error) {
	uuid, err := uuid.NewV7()
	return ObjectId(uuid), err
}

func NewObject() (obj *Object, err error) {
	uuid, err := uuid.NewV7()
	obj = &Object{
		ObjectId: ObjectId(uuid),
	}
	return
}

// Scan 实现 sql.Scanner 接口
func (id *ObjectId) Scan(value interface{}) error {
	return (*uuid.UUID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id ObjectId) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}
