package paroket

import (
	"bytes"
	"context"
	"fmt"
	"paroket/common"
	"paroket/tx"

	"github.com/tidwall/gjson"
)

type queryImpl struct {
	table  common.Table
	db     common.Database
	filter *filterNode
	sort   []SortNode
	limit  int
	offset int
}

type Operator int

const (
	opAnd Operator = iota
	opOr
	opNot

	_opVal
)

var (
	opBytesPrefix = "$"
	opBytesAnd    = "$and"
	opBytesOr     = "$or"
	opBytesNot    = "$not"
)

type filterNodeType string

const (
	Connection filterNodeType = "connection"
	Operation  filterNodeType = "operation"
)

type filterNode struct {
	Type        filterNodeType
	ChildNodes  []filterNode
	Connect     string
	filterField common.FilterField
	filterValue map[string]interface{}
}

type SortNode struct {
	SortField common.SortField
	SortValue map[string]interface{}
}

func NewQueryBuilder(table common.Table, db common.Database) common.QueryBuilder {
	return &queryImpl{
		table:  table,
		db:     db,
		sort:   []SortNode{},
		limit:  50,
		offset: 0,
	}
}

func (qb *queryImpl) ParseFilter(ctx context.Context, tx tx.ReadTx, filter string) (err error) {
	result := gjson.Parse(filter)
	switch (result.Value()).(type) {
	case interface{}:

		keys := result.Get("@keys").Array()
		if len(keys) == 0 {
			return
		}
		var filterNode *filterNode
		filterNode, err = ParseFilterHelper(ctx, qb.db, tx, result)
		qb.filter = filterNode
	default:
		err = fmt.Errorf("invaild filter")
	}
	return
}

func ParseFilterHelper(ctx context.Context, db common.Database, tx tx.ReadTx, filter gjson.Result) (node *filterNode, err error) {
	keys := filter.Get("@keys").Array()
	if len(keys) != 1 {
		err = fmt.Errorf("parse filter failed : key error %v", keys)
		return
	}
	key := keys[0].Str
	switch matchOpType(key) {
	case Connection:
		node, err = ParseConnectFilter(ctx, db, tx, filter)
	case Operation:
		node, err = ParseOperationFilter(ctx, db, tx, filter)
	default:
		err = fmt.Errorf("parse op Type error")
	}
	return
}

func ParseConnectFilter(ctx context.Context, db common.Database, tx tx.ReadTx, filter gjson.Result) (node *filterNode, err error) {
	key := filter.Get("@keys").Array()[0].Str
	node = &filterNode{
		Type:       Connection,
		Connect:    key,
		ChildNodes: []filterNode{},
	}
	childNode := filter.Get(key)
	_, ok := childNode.Value().([]interface{})
	if !ok {
		err = fmt.Errorf("connect child node is not a array")
		return
	}
	childFilterList := childNode.Array()
	for _, childFilter := range childFilterList {
		var childFilterNode *filterNode
		childFilterNode, err = ParseFilterHelper(ctx, db, tx, childFilter)
		if err != nil {
			return
		}
		node.ChildNodes = append(node.ChildNodes, *childFilterNode)
	}
	return
}

func ParseOperationFilter(ctx context.Context, db common.Database, tx tx.ReadTx, filter gjson.Result) (node *filterNode, err error) {

	key := filter.Get("@keys").Array()[0].Str
	op := filter.Get(key).Get("@keys").Array()[0].Str
	val := filter.Get(key).Get(op).Str

	switch key {
	case "$fts":
		ftsField := newFts()

		node = &filterNode{
			Type:        Operation,
			filterField: ftsField,
			filterValue: map[string]interface{}{
				"op":    op,
				"value": val,
			},
		}
	default:
		node, err = ParseAttributeOperationFilter(ctx, db, tx, filter)
	}
	return
}

func ParseAttributeOperationFilter(ctx context.Context, db common.Database, tx tx.ReadTx, filter gjson.Result) (node *filterNode, err error) {
	key := filter.Get("@keys").Array()[0].Str
	op := filter.Get(key).Get("@keys").Array()[0].Str
	val := filter.Get(key).Get(op).Value()
	var acid common.AttributeClassId
	if err = acid.Scan(key); err != nil {
		return
	}
	ac, err := db.OpenAttributeClass(ctx, tx, acid)
	if err != nil {
		return
	}
	node = &filterNode{
		Type:        Operation,
		filterField: ac,
		filterValue: map[string]interface{}{
			"op":    op,
			"value": val,
		},
	}
	return
}

func matchOpType(op string) filterNodeType {
	switch op {
	case opBytesAnd:
		return Connection
	case opBytesOr:
		return Connection
	case opBytesNot:
		return Connection
	default:
		return Operation
	}
}

func (qb *queryImpl) ParseOrder(ctx context.Context, tx tx.ReadTx, order string) (err error) {
	// result := gjson.Get(order, "")
	result := gjson.Parse(order)
	_, ok := result.Value().([]interface{})
	if !ok {
		err = fmt.Errorf("order is not a array")
		return
	}
	orderList := gjson.Parse(order).Array()
	for _, orderData := range orderList {

		parseOrderData, ok := orderData.Value().(map[string]interface{})
		if !ok {
			err = fmt.Errorf("invaild order item format")
			return
		}
		acidStr, ok := parseOrderData["field"].(string)
		if !ok {
			err = fmt.Errorf(" order item no found order field")
		}
		var acid common.AttributeClassId
		if err = acid.Scan(acidStr); err != nil {
			return
		}
		var ac common.AttributeClass
		ac, err = qb.db.OpenAttributeClass(ctx, tx, acid)
		if err != nil {
			return
		}
		delete(parseOrderData, "field")
		node := SortNode{
			SortField: ac,
			SortValue: parseOrderData,
		}
		qb.sort = append(qb.sort, node)
	}
	return
}

func (qb *queryImpl) BuildFilter(ctx context.Context, tx tx.ReadTx) (stmt string, err error) {
	stmt = ""
	if qb.filter == nil {
		return
	}
	s, err := qb.filter.BuildFilterHelper(ctx, tx)
	if err != nil {
		return
	}
	if s != "" {
		stmt = fmt.Sprintf("WHERE %s", s)
	}
	return
}

func (qb *queryImpl) BuildSort(ctx context.Context, tx tx.ReadTx) (stmt string, err error) {
	if len(qb.sort) == 0 {
		stmt = ""
		return
	}
	var buffer bytes.Buffer
	buffer.WriteString(" ORDER BY ")
	sortLen := len(qb.sort)
	for idx, sNode := range qb.sort {
		var s string
		s, err = sNode.SortField.BuildSort(ctx, tx, sNode.SortValue)
		if err != nil {
			return
		}

		buffer.WriteString(fmt.Sprintf(" %s ", s))
		if idx != sortLen-1 {
			buffer.WriteString(",")
		}
	}
	stmt = buffer.String()
	return
}

func (q *filterNode) BuildFilterHelper(ctx context.Context, tx tx.ReadTx) (stmt string, err error) {
	switch q.Type {
	case Connection:
		stmt, err = q.BuildConnect(ctx, tx)
	case Operation:
		stmt, err = q.BuildOp(ctx, tx)
	default:
		err = fmt.Errorf("unsupport queryNode type: %s from", q.Type)
	}
	return
}

func (q *filterNode) BuildConnect(ctx context.Context, tx tx.ReadTx) (stmt string, err error) {

	switch q.Connect {
	case opBytesAnd:
		stmt, err = q.andConnect(ctx, tx)
	case opBytesOr:
		stmt, err = q.orConnect(ctx, tx)
	case opBytesNot:
		stmt, err = q.notConnect(ctx, tx)
	default:
		err = fmt.Errorf("unsupport query connect type of %s", q.Connect)
	}
	return
}

func (q *filterNode) andConnect(ctx context.Context, tx tx.ReadTx) (stmt string, err error) {
	queryStmtList := []string{}
	for _, childQuery := range q.ChildNodes {
		var childStmt string
		childStmt, err = childQuery.BuildFilterHelper(ctx, tx)
		if err != nil {
			return
		}
		queryStmtList = append(queryStmtList, childStmt)
	}
	var buffer bytes.Buffer
	end := len(queryStmtList) - 1

	buffer.WriteString("(")
	for idx, childStmt := range queryStmtList {
		buffer.WriteString(childStmt)
		if idx != end {
			buffer.WriteString(" AND ")
		}

	}
	buffer.WriteString(")")
	stmt = buffer.String()
	return
}

func (q *filterNode) orConnect(ctx context.Context, tx tx.ReadTx) (stmt string, err error) {
	queryStmtList := []string{}
	for _, childQuery := range q.ChildNodes {
		var childStmt string
		childStmt, err = childQuery.BuildFilterHelper(ctx, tx)
		if err != nil {
			return
		}
		queryStmtList = append(queryStmtList, childStmt)
	}
	var buffer bytes.Buffer
	end := len(queryStmtList) - 1
	buffer.WriteString("(")
	for idx, childStmt := range queryStmtList {
		buffer.WriteString(childStmt)
		if idx != end {
			buffer.WriteString(" OR ")
		}

	}
	buffer.WriteString(")")
	stmt = buffer.String()
	return
}

func (q *filterNode) notConnect(ctx context.Context, tx tx.ReadTx) (stmt string, err error) {
	queryStmtList := []string{}
	for _, childQuery := range q.ChildNodes {
		var childStmt string
		childStmt, err = childQuery.BuildFilterHelper(ctx, tx)
		if err != nil {
			return
		}
		queryStmtList = append(queryStmtList, childStmt)
	}
	var buffer bytes.Buffer
	end := len(queryStmtList) - 1
	buffer.WriteString(" NOT (")
	for idx, childStmt := range queryStmtList {
		buffer.WriteString(childStmt)
		if idx != end {
			buffer.WriteString(" AND ")
		}

	}
	buffer.WriteString(")")
	stmt = buffer.String()
	return
}

func (q *filterNode) BuildOp(ctx context.Context, tx tx.ReadTx) (stmt string, err error) {
	stmt, err = q.filterField.BuildQuery(ctx, tx, q.filterValue)
	return
}
