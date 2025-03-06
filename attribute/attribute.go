package attribute

import (
	"context"
	"database/sql"
	"fmt"
	"paroket/common"
	"paroket/tx"
	"paroket/utils"
)

// 公用实现
type AttributeClassInfo struct {
	db       common.Database
	id       common.AttributeClassId
	name     string
	key      string
	attrType common.AttributeType
	metaInfo utils.JSONMap
}

type AttributeClassInterface struct {
	Create func(ctx context.Context, db common.Database, tx tx.WriteTx) (attr common.AttributeClass, err error)
	Parse  func(ctx context.Context, tx tx.ReadTx, acProto *AttributeClassInfo) (ac common.AttributeClass, err error)
}

var AttributeClassMap = map[common.AttributeType]*AttributeClassInterface{}

func init() {
	RegisterAttributeClass(AttributeTypeText, newTextAttributeClass, parseTextAttributeClass)

	RegisterAttributeClass(AttributeTypeNumber, newNumberAttributeClass, parseNumberAttributeClass)

	RegisterAttributeClass(AttributeTypeLink, newLinkAttributeClass, parseLinkAttributeClass)

}

func RegisterAttributeClass(attrType common.AttributeType,
	create func(ctx context.Context, db common.Database, tx tx.WriteTx) (attr common.AttributeClass, err error),
	parse func(ctx context.Context, tx tx.ReadTx, acProto *AttributeClassInfo) (ac common.AttributeClass, err error),
) (err error) {
	if create == nil {
		err = fmt.Errorf("attributeClass create is nil")
		return
	}
	if parse == nil {
		err = fmt.Errorf("attributeClass parse is nil")
		return
	}
	AttributeClassMap[attrType] = &AttributeClassInterface{
		Create: create,
		Parse:  parse,
	}
	return
}

func NewAttributeClass(ctx context.Context, db common.Database, tx tx.WriteTx, attributbuteType common.AttributeType) (attr common.AttributeClass, err error) {
	acInterface, ok := AttributeClassMap[attributbuteType]
	if !ok {
		return nil, fmt.Errorf("unsupport type %s", attributbuteType)
	}
	return acInterface.Create(ctx, db, tx)
}

func QueryAttributeClass(ctx context.Context, db common.Database, tx tx.ReadTx, acid common.AttributeClassId) (ac common.AttributeClass, err error) {
	var acProto AttributeClassInfo
	acProto.db = db
	stmt := `SELECT
	  class_id, attribute_name, attribute_key, attribute_type, attribute_meta_info
	  FROM attribute_classes
	  WHERE class_id = ?`
	err = tx.QueryRow(stmt, acid).Scan(&acProto.id, &acProto.name, &acProto.key, &acProto.attrType, &acProto.metaInfo)
	if err == sql.ErrNoRows {
		err = fmt.Errorf("%w:%w", common.ErrAttributeClassNotFound, sql.ErrNoRows)
		return
	}
	acInterface, ok := AttributeClassMap[acProto.attrType]
	if !ok {
		return nil, fmt.Errorf("unsupport type form database %s", acProto.attrType)
	}
	return acInterface.Parse(ctx, tx, &acProto)
}

// 一些attributeClass的公用实现
func (t *AttributeClassInfo) Name() string {
	return t.name
}

func (t *AttributeClassInfo) Type() common.AttributeType {
	return t.attrType
}
func (t *AttributeClassInfo) ClassId() common.AttributeClassId {
	return t.id
}

func (t *AttributeClassInfo) Key() string {
	return t.key
}

func (t *AttributeClassInfo) DoPreHook(ctx context.Context, db common.Database, tx tx.WriteTx, op common.AttributeOp) (err error) {
	fList := common.ListPreAttributeHook()
	for _, f := range fList {
		nerr := f(ctx, db, tx, op)
		if nerr != nil {
			if err != nil {
				err = fmt.Errorf("%w:%w", err, nerr)
			}
			err = nerr
		}
	}
	return
}
func (t *AttributeClassInfo) DoAfterHook(ctx context.Context, db common.Database, tx tx.WriteTx, op common.AttributeOp) (err error) {
	fList := common.ListAfterAttributeHook()
	for _, f := range fList {
		nerr := f(ctx, db, tx, op)
		if nerr != nil {
			if err != nil {
				err = fmt.Errorf("%w:%w", err, nerr)
			}
			err = nerr
		}
	}
	return
}

const (
	AttributeTypeText   common.AttributeType = "text"
	AttributeTypeNumber common.AttributeType = "number"
	AttributeTypeLink   common.AttributeType = "link"
)

type attrOp struct {
	classId   common.AttributeClassId
	obj       common.Object
	op        common.AttributeOpType
	attribute common.Attribute
}

func NewOp(cid common.AttributeClassId, obj common.Object, op common.AttributeOpType, attr common.Attribute) common.AttributeOp {
	return &attrOp{
		classId:   cid,
		obj:       obj,
		op:        op,
		attribute: attr,
	}
}

func (op *attrOp) ClassId() common.AttributeClassId {
	return op.classId
}

func (op *attrOp) Object() common.Object {
	return op.obj
}

func (op *attrOp) Op() common.AttributeOpType {
	return op.op
}

func (op *attrOp) Attribute() common.Attribute {
	return op.attribute
}
