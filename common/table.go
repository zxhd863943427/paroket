package common

import (
	"context"
	"paroket/tx"
	"paroket/utils"
)

type Table interface {
	Name() string

	TableId() TableId

	MetaInfo() utils.JSONMap

	Set(ctx context.Context, tx tx.WriteTx, v utils.JSONMap) (err error)

	FindId(ctx context.Context, tx tx.ReadTx, oidList ...ObjectId) ([]Object, error)

	Insert(ctx context.Context, tx tx.WriteTx, oidList ...ObjectId) error

	Delete(ctx context.Context, tx tx.WriteTx, oidList ...ObjectId) error

	AddAttributeClass(ctx context.Context, tx tx.WriteTx, ac AttributeClass) error

	DeleteAttributeClass(ctx context.Context, tx tx.WriteTx, ac AttributeClass) error

	Find(ctx context.Context, tx tx.ReadTx, query TableQuery) ([]Object, error)

	NewView(ctx context.Context, tx tx.WriteTx) (View, error)

	GetViewData(ctx context.Context, tx tx.ReadTx, view View, config QueryConfig) ([][]Attribute, error)

	DropTable(ctx context.Context, tx tx.WriteTx) error
}
