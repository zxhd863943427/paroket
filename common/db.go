package common

import (
	"context"
	"paroket/tx"
)

type DB interface {
	Database

	// 打开数据库
	Open(ctx context.Context, dbPath string, config *Config) error

	// DB操作
	ReadTx(ctx context.Context) (tx.ReadTx, error)

	WriteTx(ctx context.Context) (tx.WriteTx, error)

	// 关闭数据库
	Close(ctx context.Context) error
}

type Database interface {
	// AttributeClass操作
	CreateAttributeClass(ctx context.Context, tx tx.WriteTx, AttrType AttributeType) (ac AttributeClass, err error)

	OpenAttributeClass(ctx context.Context, tx tx.ReadTx, acid AttributeClassId) (ac AttributeClass, err error)

	ListAttributeClass(ctx context.Context, tx tx.ReadTx) (acList []AttributeClass, err error)

	DeleteAttributeClass(ctx context.Context, tx tx.WriteTx, acid AttributeClassId) (err error)

	// Object操作
	CreateObject(ctx context.Context, tx tx.WriteTx) (obj Object, err error)

	OpenObject(ctx context.Context, tx tx.ReadTx, oid ObjectId) (obj Object, err error)

	DeleteObject(ctx context.Context, tx tx.WriteTx, oid ObjectId) (err error)

	// Table 操作
	CreateTable(ctx context.Context, tx tx.WriteTx) (Table, error)

	OpenTable(ctx context.Context, tx tx.ReadTx, tid TableId) (Table, error)

	DeleteTable(ctx context.Context, tx tx.WriteTx, tid TableId) error
}
