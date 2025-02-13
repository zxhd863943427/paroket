package common

import (
	"context"
	"paroket/utils"
)

type AttributeClass interface {
	NewAttribute() (attr Attribute, err error)
	Name() string
	Type() AttributeType
	ClassId() AttributeClassId
	GetJsonValuePath() string
	Set(ctx context.Context, v utils.JSONMap) (err error)
	FindId(ctx context.Context, oid ObjectId) (attr Attribute, err error)
	Upsert(ctx context.Context, oid ObjectId, attr Attribute) (err error)
	Delete(ctx context.Context, oid ObjectId) (err error)

	//构建查询
	BuildQuery(ctx context.Context, v map[string]interface{}) (string, error)
	//构建排序
	BuildSort(ctx context.Context, v map[string]interface{}) (string, error)
}

type Attribute interface {
	GetJSON() string                       //获取值的JSON表示
	String() string                        //获得值的string表示
	GetType() string                       //获取class ID
	GetClassId() AttributeClassId          //获取class ID
	SetValue(map[string]interface{}) error //设置值
}

type AttributeType string
