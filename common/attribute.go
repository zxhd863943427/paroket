package common

import (
	"context"
	"paroket/utils"
)

type AttributeClass interface {
	Name() string
	Type() AttributeType
	ClassId() AttributeClassId
	GetMetaInfo(ctx context.Context) (v utils.JSONMap, err error)
	Set(ctx context.Context, v utils.JSONMap) (err error)
	Insert(ctx context.Context, oid ObjectId) (attr Attribute, err error)
	FindId(ctx context.Context, oid ObjectId) (attr Attribute, err error)
	Update(ctx context.Context, oid ObjectId, attr Attribute) (err error)
	Delete(ctx context.Context, oid ObjectId) (err error)
	Drop(ctx context.Context) (err error) //删除属性类

	//构建查询
	BuildQuery(ctx context.Context, v map[string]interface{}) (string, error)
	//构建排序
	BuildSort(ctx context.Context, v map[string]interface{}) (string, error)
}

type Attribute interface {
	GetJSON() string                       //获取值的JSON表示
	String() string                        //获得值的string表示
	GetClass() AttributeClass              //获取class
	SetValue(map[string]interface{}) error //设置值
	Parse(string) error                    //从读取的json正确解析
}

type AttributeType string
