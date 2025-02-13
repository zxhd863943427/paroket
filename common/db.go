package common

import (
	"context"
	"paroket/tx"
)

type DB interface {
	// 打开数据库
	Open(ctx context.Context, dbPath string, config *Config) error

	// AttributeClass操作
	CreateAttributeClass(ctx context.Context, AttrType Attribute) (ac AttributeClass, err error)

	OpenAttributeClass(ctx context.Context, acid AttributeClassId) (ac AttributeClass, err error)

	ListAttributeClass(ctx context.Context) (ac AttributeClass, err error)

	// Object操作
	CreateObject(ctx context.Context) (obj *Object, err error)

	OpenObject(ctx context.Context, oid ObjectId) (obj *Object, err error)

	// Table 操作
	CreateTable(ctx context.Context) (Table, error)

	OpenTable(ctx context.Context, tid TableId) (Table, error)

	Table(ctx context.Context) (Table, error)

	// DB操作
	ReadTx(ctx context.Context) (tx.ReadTx, error)

	WriteTx(ctx context.Context) (tx.WriteTx, error)

	// 关闭数据库
	Close(ctx context.Context) error
}
