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
	db       common.DB
	id       common.AttributeClassId
	name     string
	key      string
	attrType common.AttributeType
	metaInfo utils.JSONMap
}

func NewAttributeClass(ctx context.Context, db common.DB, attributbuteType common.AttributeType) (attr common.AttributeClass, err error) {
	switch attributbuteType {
	case AttributeTypeText:
		return newTextAttributeClass(ctx, db)
	default:
		return nil, fmt.Errorf("unsupport type %s", attributbuteType)
	}
}

func QueryAttributeClass(ctx context.Context, db common.DB, acid common.AttributeClassId) (ac common.AttributeClass, err error) {
	var acProto attributeClass
	acProto.db = db
	func() {
		var tx tx.ReadTx
		tx, err = db.ReadTx(ctx)
		if err != nil {
			return
		}
		defer tx.Commit()

		stmt := `SELECT 
	  class_id, attribute_name, attribute_key, attribute_type, attribute_meta_info 
	  FROM attribute_classes
	  WHERE
	  class_id = ?`
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
