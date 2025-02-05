package object

import (
	"database/sql/driver"

	"github.com/rs/xid"
)

type ObjectId xid.ID

type Object struct {
	ObjectId ObjectId
}

func NewObjectId() (ObjectId, error) {
	guid := xid.New()
	return ObjectId(guid), nil
}

func NewObject() (obj *Object, err error) {
	guid := xid.New()
	obj = &Object{
		ObjectId: ObjectId(guid),
	}
	return
}

// Scan 实现 sql.Scanner 接口
func (id *ObjectId) Scan(value interface{}) error {
	return (*xid.ID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id ObjectId) Value() (driver.Value, error) {
	return xid.ID(id).Value()
}
