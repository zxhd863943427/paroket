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
	Parse  func(ctx context.Context, acProto *AttributeClassInfo) (ac common.AttributeClass, err error)
}

var AttributeClassMap = map[common.AttributeType]*AttributeClassInterface{}

func init() {
	RegisterAttributeClass(AttributeTypeText, newTextAttributeClass, parseTextAttributeClass)
}

func RegisterAttributeClass(attrType common.AttributeType,
	create func(ctx context.Context, db common.Database, tx tx.WriteTx) (attr common.AttributeClass, err error),
	parse func(ctx context.Context, acProto *AttributeClassInfo) (ac common.AttributeClass, err error),
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
	return acInterface.Parse(ctx, &acProto)
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

const (
	AttributeTypeText common.AttributeType = "text"
)
