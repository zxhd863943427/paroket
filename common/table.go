package common

import (
	"context"
	"paroket/utils"
)

type Table interface {
	Name() string

	TableId() TableId

	MetaInfo() utils.JSONMap

	Set(ctx context.Context, v utils.JSONMap) (err error)

	FindId(ctx context.Context, oidList ...ObjectId) ([]*Object, error)

	Insert(ctx context.Context, oidList ...ObjectId) error

	Delete(ctx context.Context, oidList ...ObjectId) error

	AddAttributeClass(ctx context.Context, ac AttributeClass) error

	DeleteAttributeClass(ctx context.Context, ac AttributeClass) error

	Find(ctx context.Context, query TableQuery) ([]*Object, error)

	NewView(ctx context.Context) (View, error)

	GetViewData(ctx context.Context, view View, config QueryConfig) ([][]Attribute, error)

	DropTable(ctx context.Context) error
}
