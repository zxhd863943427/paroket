package query

import (
	"database/sql"
)

type Query struct {
	db sql.DB
}

type queryBuilder struct {
	tableId string
	fields  []string
	sorts   []Sort
	Joins   []Join
	limit   int
	offset  int
}

type Sort struct {
	attributeClassId   string
	attributeClassType string
	metaInfo           map[string]interface{}
	reverse            bool
}

type Join struct {
	attributeClassId   string
	attributeClassType string
	metaInfo           map[string]interface{}
	bounds             []Bound
}

type Bound struct {
	Operator string
	Value    interface{}
}
