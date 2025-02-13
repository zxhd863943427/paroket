package attribute

import (
	"database/sql"
	"fmt"
	"paroket/common"
	"paroket/object"
	"paroket/utils"
	"time"

	"github.com/rs/xid"
)

// 公用实现
type attributeClass struct {
	db                common.DB
	classId           common.AttributeClassId
	attributeName     string
	attributeType     AttributeType
	attributeMetaInfo utils.JSONMap
	impl              attributeClassImpl
}

type attributeClassFieldMap struct{}

func (am attributeClassFieldMap) ClassId() string           { return `class_id` }
func (am attributeClassFieldMap) AttributeName() string     { return `attribute_name` }
func (am attributeClassFieldMap) AttributeType() string     { return `attribute_type` }
func (am attributeClassFieldMap) AttributeMetaInfo() string { return `attribute_meta_info` }

var AttributeClassFieldMap = attributeClassFieldMap{}

func AttributeClassField() string {
	return fmt.Sprintf(
		` %s, %s, %s, %s `,
		AttributeClassFieldMap.ClassId(),
		AttributeClassFieldMap.AttributeName(),
		AttributeClassFieldMap.AttributeType(),
		AttributeClassFieldMap.AttributeMetaInfo(),
	)
}

func InsertField() string {
	return `(?, ?, ?, ?)`
}

func NewAttributeClass(attributbuteType AttributeType) (ac AttributeClass, err error) {
	guid := xid.New()
	cid := AttributeClassId(guid)
	switch attributbuteType {
	case AttributeTypeText:
		act := &attributeClass{
			classId:           cid,
			attributeName:     "untitled",
			attributeType:     AttributeTypeText,
			attributeMetaInfo: map[string]interface{}{},
		}
		act.impl = &attribute.TextAttributeClass{AttributeClass: ac}
		ac = act
	default:
		err = fmt.Errorf("unsupport attribute type of %s", attributbuteType)
		return
	}

	return
}

func (ac *attributeClass) SearchByID(tx *sql.Tx, objId object.ObjectId) (attr Attribute, err error) {
	return ac.impl.SearchByID(tx, objId)
}

func (ac *attributeClass) NewAttribute() (attr Attribute, err error) {
	return ac.impl.NewAttribute()
}

func (acid AttributeClassId) String() string {
	guid := xid.ID(acid)
	return guid.String()
}

type AttributeType string

const (
	AttributeTypeText AttributeType = "text"
)

type AttributeStore struct {
	ObjectId      []byte
	AttributeId   AttributeId
	AttributeType string
	UpdateDate    time.Time
	Data          string
}
