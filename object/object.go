package object

import (
	"paroket/attribute"
	"paroket/table"
)

type ObjectId string

type Object struct {
	ObjectId   ObjectId
	Attributes map[attribute.AttributeId]bool
	Tables     map[table.TableId]bool
}
