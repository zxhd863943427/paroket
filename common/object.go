package common

import (
	"paroket/utils"

	"github.com/rs/xid"
)

type Object struct {
	ObjectId ObjectId
	Value    []byte
}
type ObjectRelationTable struct {
	Tables utils.JSONMap
}

func NewObject() (obj *Object, err error) {
	guid := xid.New()
	obj = &Object{
		ObjectId: ObjectId(guid),
		Value:    []byte{},
	}
	return
}
