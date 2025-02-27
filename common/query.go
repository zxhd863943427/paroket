package common

import (
	"context"
	"paroket/tx"
)

type Query interface {
	Filter(ctx context.Context, tx tx.ReadTx, filter string) (q Query)
	OrderBy(ctx context.Context, tx tx.ReadTx, order string) (q Query)
	Limit(limit int) (q Query)
	Offset(offset int) (q Query)
	Find(ctx context.Context, tx tx.ReadTx) (objList []Object, err error)
}

type QueryBuilder interface {
	ParseFilter(ctx context.Context, tx tx.ReadTx, filter string) (err error)
	ParseOrder(ctx context.Context, tx tx.ReadTx, order string) (err error)
	BuildFilter(ctx context.Context, tx tx.ReadTx) (stmt string, err error)
	BuildSort(ctx context.Context, tx tx.ReadTx) (stmt string, err error)
}

type FilterField interface {
	BuildQuery(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (string, error)
}

type SortField interface {
	BuildSort(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (string, error)
}
