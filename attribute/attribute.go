package attribute

import (
	"context"
	"database/sql"
	"fmt"
	"paroket/common"
	"paroket/tx"
	"paroket/utils"

	"github.com/pkg/errors"
)

// 公用实现
type attributeClass struct {
	db       common.Database
	id       common.AttributeClassId
	name     string
	key      string
	attrType common.AttributeType
	metaInfo utils.JSONMap
}

func NewAttributeClass(ctx context.Context, db common.Database, tx tx.WriteTx, attributbuteType common.AttributeType) (attr common.AttributeClass, err error) {
	switch attributbuteType {
	case AttributeTypeText:
		return newTextAttributeClass(ctx, db, tx)
	default:
		return nil, fmt.Errorf("unsupport type %s", attributbuteType)
	}
}

func QueryAttributeClass(ctx context.Context, db common.Database, tx tx.ReadTx, acid common.AttributeClassId) (ac common.AttributeClass, err error) {
	var acProto attributeClass
	acProto.db = db
	func() {

		stmt := `SELECT 
	  class_id, attribute_name, attribute_key, attribute_type, attribute_meta_info 
	  FROM attribute_classes
	  WHERE class_id = ?`
		err = tx.QueryRow(stmt, acid).Scan(&acProto.id, &acProto.name, &acProto.key, &acProto.attrType, &acProto.metaInfo)
		if err == sql.ErrNoRows {
			err = errors.Wrapf(err, "%w", common.ErrAttributeClassNotFound)
		}
	}()
	if err != nil {
		return
	}

	switch acProto.attrType {
	case AttributeTypeText:
		ac, err = parseTextAttributeClass(ctx, &acProto)
		return
	default:
		err = fmt.Errorf("unsupport type form database")
		return
	}
}

const (
	AttributeTypeText common.AttributeType = "text"
)
