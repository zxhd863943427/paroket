package common

import (
	"bytes"
	"context"
	"fmt"
	"paroket/tx"
)

type TableResult interface {
	Raw() []Object
	RawData(ctx context.Context, tx tx.ReadTx) (ret []Result, err error)
	Marshal(ctx context.Context, tx tx.ReadTx) (ret string, err error)
}

type Result struct {
	oid  ObjectId
	data []Attribute
}

type tableResultImpl struct {
	db      Database
	fields  []AttributeClassId
	objList []Object
}

func NewTableResult(db Database, fields []AttributeClassId, objList []Object) (ret TableResult) {
	ret = &tableResultImpl{
		db:      db,
		fields:  fields,
		objList: objList,
	}
	return
}

func (v *tableResultImpl) Raw() []Object {
	return v.objList
}

func (v *tableResultImpl) RawData(ctx context.Context, tx tx.ReadTx) (ret []Result, err error) {
	ret = []Result{}
	acList := []AttributeClass{}
	for _, acid := range v.fields {
		var ac AttributeClass
		ac, err = v.db.OpenAttributeClass(ctx, tx, acid)
		if err != nil {
			return
		}
		acList = append(acList, ac)
	}
	for _, obj := range v.objList {
		attrList := []Attribute{}
		for _, ac := range acList {
			var attr Attribute
			attr, err = ac.FromObject(obj)
			if err != nil {
				return
			}
			attrList = append(attrList, attr)

		}
		ret = append(ret, Result{
			oid:  obj.ObjectId(),
			data: attrList,
		})
	}
	return

}

func (v *tableResultImpl) Marshal(ctx context.Context, tx tx.ReadTx) (ret string, err error) {
	resultList, err := v.RawData(ctx, tx)
	if err != nil {
		return
	}
	retBuffer := &bytes.Buffer{}
	retBuffer.WriteString("[")
	for idx, result := range resultList {
		if idx != 0 {
			retBuffer.WriteString(",")
		}

		retBuffer.WriteString("{")

		objectIdData := fmt.Sprintf(`"object_id":"%v"`, result.oid)
		retBuffer.WriteString(objectIdData)

		for _, attr := range result.data {
			retBuffer.WriteString(",")
			attrData := fmt.Sprintf(`"%v":%s`, attr.GetClass().ClassId(), attr.GetJSON())
			retBuffer.WriteString(attrData)
		}

		retBuffer.WriteString("}")

	}
	retBuffer.WriteString("]")
	ret = retBuffer.String()
	return
}
