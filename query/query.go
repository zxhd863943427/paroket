package query

import (
	"bytes"
	"context"
	"fmt"
	"paroket/common"
	"paroket/tx"
)

type filterField interface {
	BuildQuery(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (string, error)
}

type SortField interface {
	BuildSort(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (string, error)
}

type queryImpl struct {
	table  common.Table
	filter *filterNode
	sort   []SortNode
	limit  int
	offset int
}

type filterNodeType string

const (
	Connection filterNodeType = "connection"
	Operation  filterNodeType = "operation"
)

type filterNode struct {
	Type        filterNodeType
	ChildNodes  []filterNode
	Op          string
	Connect     string
	filterField filterField
	filterValue map[string]interface{}
}

type SortNode struct {
	SortField SortField
	SortValue map[string]interface{}
}

func NewQueryBuilder(table common.Table) common.QueryBuilder {
	return &queryImpl{
		table:  table,
		sort:   []SortNode{},
		limit:  50,
		offset: 0,
	}
}

func (qb *queryImpl) ParseFilter(ctx context.Context, tx tx.ReadTx, filter string) (err error) {
	return
}
func (qb *queryImpl) ParseOrder(ctx context.Context, tx tx.ReadTx, order string) (err error) {
	return
}

func (qb *queryImpl) setQuery(q *filterNode) *queryImpl {
	qb.filter = q
	return qb
}

func (qb *queryImpl) addSort(slist []SortNode) *queryImpl {
	qb.sort = append(qb.sort, slist...)
	return qb
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
	case "and":
		stmt, err = q.andConnect(ctx, tx)
	case "or":
		stmt, err = q.orConnect(ctx, tx)
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

func (q *filterNode) BuildOp(ctx context.Context, tx tx.ReadTx) (stmt string, err error) {
	stmt, err = q.filterField.BuildQuery(ctx, tx, q.filterValue)
	return
}
