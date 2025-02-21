package common

import (
	"paroket/utils"

	"github.com/rs/xid"
)

type Object struct {
	ObjectId ObjectId
	Data     []byte
}
type ObjectRelationTable struct {
	Tables utils.JSONMap
}

func NewObject() (obj *Object, err error) {
	guid := xid.New()
	obj = &Object{
		ObjectId: ObjectId(guid),
		Data:     []byte("{}"),
	}
	return
}
