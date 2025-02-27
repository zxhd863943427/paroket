package common

import (
	"context"
	"paroket/tx"
)

type View interface {
	ViewId() ViewId
	Filter(tx tx.WriteTx, filter string) (err error)
	SortBy(tx tx.WriteTx, order string) (err error)
	Limit(limit int) (v View)
	Offset(offset int) (v View)
	Set(v map[string]interface{}) (err error)
	Query(ctx context.Context, tx tx.ReadTx) (queryData TableResult, err error)
	Marshal() string
}
