package attribute

import (
	"database/sql/driver"
	"paroket/utils"
	"strings"

	"github.com/google/uuid"
)

type AttributeId uuid.UUID

type AttributeClassId uuid.UUID

// Scan 实现 sql.Scanner 接口
func (id *AttributeClassId) Scan(value interface{}) error {
	return (*uuid.UUID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id AttributeClassId) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}

// Scan 实现 sql.Scanner 接口
func (id *AttributeId) Scan(value interface{}) error {
	return (*uuid.UUID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id AttributeId) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}

type AttributeClass struct {
	ClassId           AttributeClassId
	AttributeName     string
	AttributeType     string
	AttributeMetaInfo utils.JSONMap
}

func (acid AttributeClassId) String() string {
	uuid := uuid.UUID(acid)
	return strings.ReplaceAll(uuid.String(), "-", "_")
}

func NewAttributeClass(attributbuteType string) (ac *AttributeClass, err error) {
	uuid, err := uuid.NewV7()
	if err != nil {
		return
	}
	ac = &AttributeClass{
		ClassId:           AttributeClassId(uuid),
		AttributeName:     "untitled",
		AttributeType:     attributbuteType,
		AttributeMetaInfo: map[string]interface{}{},
	}

	return
}

type Attribute interface {
	GetId() AttributeId
	GetJSON() string
	GetType() string
}

const (
	AttributeTypeText = "text"
)
