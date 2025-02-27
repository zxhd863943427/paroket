package common

import (
	"context"
	"paroket/tx"
	"paroket/utils"
)

type AttributeClass interface {
	Name() string
	Type() AttributeType
	ClassId() AttributeClassId
	GetMetaInfo(ctx context.Context, tx tx.ReadTx) (v utils.JSONMap, err error)
	Set(ctx context.Context, tx tx.WriteTx, v utils.JSONMap) (err error)
	Insert(ctx context.Context, tx tx.WriteTx, oid ObjectId) (attr Attribute, err error)
	FindId(ctx context.Context, tx tx.ReadTx, oid ObjectId) (attr Attribute, err error)
	Update(ctx context.Context, tx tx.WriteTx, oid ObjectId, attr Attribute) (err error)
	Delete(ctx context.Context, tx tx.WriteTx, oid ObjectId) (err error)
	Drop(ctx context.Context, tx tx.WriteTx) (err error) //删除属性类
	FromObject(obj Object) (Attribute, error)            //从Object中解析中attribute

	//构建查询
	FilterField
	//构建排序
	SortField
}

type Attribute interface {
	GetJSON() string                       //获取值的JSON表示
	String() string                        //获得值的string表示
	GetClass() AttributeClass              //获取class
	SetValue(map[string]interface{}) error //设置值
	Parse(string) error                    //从读取的json正确解析
}

type AttributeType string
