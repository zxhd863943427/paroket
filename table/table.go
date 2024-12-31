package table

import (
	"paroket/utils"
	"strings"

	"github.com/google/uuid"
)

type TableId uuid.UUID

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
