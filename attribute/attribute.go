package attribute

import "github.com/google/uuid"

type AttributeId uuid.UUID

type AttributeClassId uuid.UUID

type AttributeClass struct {
	Id            AttributeClassId
	AttributeType string
	Description   string
}

type Attribute interface {
	GetId() AttributeId
	GetValue() string
}
