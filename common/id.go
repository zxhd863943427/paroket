package common

import (
	"database/sql/driver"

	"github.com/rs/xid"
)

type TableId xid.ID

type ObjectId xid.ID

type AttributeId xid.ID

type AttributeClassId xid.ID

// Scan 实现 sql.Scanner 接口
func (id *TableId) Scan(value interface{}) error {
	return (*xid.ID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id TableId) Value() (driver.Value, error) {
	return xid.ID(id).Value()
}

func (tid TableId) String() string {
	guid := xid.ID(tid)
	return guid.String()
}

func NewTableId() (TableId, error) {
	guid := xid.New()
	return TableId(guid), nil
}

// Scan 实现 sql.Scanner 接口
func (id *ObjectId) Scan(value interface{}) error {
	return (*xid.ID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id ObjectId) Value() (driver.Value, error) {
	return xid.ID(id).Value()
}

func (oid ObjectId) String() string {
	guid := xid.ID(oid)
	return guid.String()
}

func NewObjectId() (ObjectId, error) {
	guid := xid.New()
	return ObjectId(guid), nil
}

// Scan 实现 sql.Scanner 接口
func (id *AttributeClassId) Scan(value interface{}) error {
	return (*xid.ID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id AttributeClassId) Value() (driver.Value, error) {
	return xid.ID(id).Value()
}

func (acid AttributeClassId) String() string {
	guid := xid.ID(acid)
	return guid.String()
}

func NewAttributeClassId() (AttributeClassId, error) {
	guid := xid.New()
	return AttributeClassId(guid), nil
}

// Scan 实现 sql.Scanner 接口
func (id *AttributeId) Scan(value interface{}) error {
	return (*xid.ID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id AttributeId) Value() (driver.Value, error) {
	return xid.ID(id).Value()
}

func (aid AttributeId) String() string {
	guid := xid.ID(aid)
	return guid.String()
}

func NewAttributeId() (AttributeId, error) {
	guid := xid.New()
	return AttributeId(guid), nil
}
