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

	NewView(ctx context.Context, tx tx.WriteTx) (View, error)

	ListView(ctx context.Context, tx tx.ReadTx) ([]View, error)

	View(ctx context.Context, tx tx.ReadTx, vid ViewId) (View, error)

	GetViewData(ctx context.Context, tx tx.ReadTx, vid ViewId) (TableResult, error)

	DropTable(ctx context.Context, tx tx.WriteTx) error
}
