package attribute

import (
	"database/sql/driver"
	"fmt"
	"paroket/utils"
	"strings"
	"time"

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

func NewAttributeClassId() (AttributeClassId, error) {
	uuid, err := uuid.NewV7()
	return AttributeClassId(uuid), err
}

func NewAttributeId() (AttributeId, error) {
	uuid, err := uuid.NewV7()
	return AttributeId(uuid), err
}

func (ac *AttributeClass) GetDataTableName() string {
	return fmt.Sprintf(
		`%s_%s`,
		ac.AttributeType,
		ac.ClassId.String())
}

func (ac *AttributeClass) GetDataIndexName() string {
	return fmt.Sprintf(
		`%s_%s_index`,
		ac.AttributeType,
		ac.ClassId.String(),
	)
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

type AttributeStore struct {
	ObjectId      uuid.UUID
	AttributeId   uuid.UUID
	AttributeType string
	UpdateDate    time.Time
	Data          string
}
