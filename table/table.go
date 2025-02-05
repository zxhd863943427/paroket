package table

import (
	"database/sql/driver"
	"fmt"
	"paroket/object"
	"paroket/utils"

	"github.com/rs/xid"
)

type TableId xid.ID

func NewTableId() (TableId, error) {
	guid := xid.New()
	return TableId(guid), nil
}

// Scan 实现 sql.Scanner 接口
func (id *TableId) Scan(value interface{}) error {
	return (*xid.ID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id TableId) Value() (driver.Value, error) {
	return xid.ID(id).Value()
}

type Table struct {
	TableId   TableId
	TableName string
	MetaInfo  utils.JSONMap
	Version   int64
}

func (tid TableId) String() string {
	guid := xid.ID(tid)
	return guid.String()
}

func (tid TableId) GetTableName() string {
	guid := xid.ID(tid)
	str := guid.String()
	return fmt.Sprintf("table_%s", str)
}

func NewTable() (t *Table, err error) {
	tid, err := NewTableId()
	t = &Table{
		TableId:  TableId(tid),
		MetaInfo: utils.JSONMap{},
		Version:  0,
	}
	return
}

type TableValue struct {
	ObjectId object.ObjectId
	Values   map[string]string
}

func (tid TableId) CreateMaterialized() {
	//TODO
}
