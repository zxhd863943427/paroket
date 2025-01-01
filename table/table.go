package table

import (
	"database/sql/driver"
	"paroket/utils"
	"strings"

	"github.com/google/uuid"
)

type TableId uuid.UUID

func NewTableId() (TableId, error) {
	uuid, err := uuid.NewV7()
	return TableId(uuid), err
}

// Scan 实现 sql.Scanner 接口
func (id *TableId) Scan(value interface{}) error {
	return (*uuid.UUID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id TableId) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}

type Table struct {
	TableId   TableId
	TableName string
	MetaInfo  utils.JSONMap
	Version   int64
}

func (tid TableId) String() string {
	uuid := uuid.UUID(tid)
	return strings.ReplaceAll(uuid.String(), "-", "_")
}

func NewTable() (t *Table, err error) {
	uuid, err := uuid.NewV7()
	t = &Table{
		TableId:  TableId(uuid),
		MetaInfo: utils.JSONMap{},
		Version:  0,
	}
	return
}
