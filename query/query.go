package query

import (
	"bytes"
	"fmt"
	"paroket/table"
)

type QueryField interface {
	BuildQuery(v map[string]interface{}) (string, error)
}

type SortField interface {
	BuildSort(v map[string]interface{}) (string, error)
}

type QueryMode string

const (
	TableMode QueryMode = "table"
)

type Query struct {
	tableId table.TableId
	mode    QueryMode
	fields  []string
	query   *QueryNode
	sort    []SortNode
	limit   int
	offset  int
}

type QueryNodeType string

const (
	Connection QueryNodeType = "connection"
	Operation  QueryNodeType = "operation"
)

type QueryNode struct {
	Type       QueryNodeType
	ChildNodes []QueryNode
	Op         string
	Connect    string
	QueryField QueryField
	QueryValue map[string]interface{}
}

type SortNode struct {
	SortField SortField
	SortValue map[string]interface{}
}

func NewQueryBuilder(tid table.TableId) *Query {
	return &Query{
		tableId: tid,
		fields:  []string{},
		sort:    []SortNode{},
		limit:   50,
		offset:  0,
	}
}

func (qb *Query) AddFields(flist []string) *Query {
	qb.fields = append(qb.fields, flist...)
	return qb
}

func (qb *Query) AddQuery(q *QueryNode) *Query {
	qb.query = q
	return qb
}

func (qb *Query) AddSort(slist []SortNode) *Query {
	qb.sort = append(qb.sort, slist...)
	return qb
}

func (qb *Query) buildQuery() (stmt string, err error) {
	stmt = ""
	if qb.query == nil {
		return
	}
	s, err := qb.query.BuildQuerySQL()
	if err != nil {
		return
	}
	if s != "" {
		stmt = fmt.Sprintf("WHERE %s", s)
	}
	return
}

func (qb *Query) buildSort() (stmt string, err error) {
	if len(qb.sort) == 0 {
		stmt = ""
		return
	}
	var buffer bytes.Buffer
	buffer.WriteString(" ORDER BY ")
	sortLen := len(qb.sort)
	for idx, sNode := range qb.sort {
		var s string
		s, err = sNode.SortField.BuildSort(sNode.SortValue)
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

func (qb *Query) Build() (stmt string, err error) {
	var buffer bytes.Buffer
	tableName := qb.tableId.GetTableName()
	buffer.WriteString(fmt.Sprintf("SELECT %s.object_id AS object_id", tableName))
	for _, field := range qb.fields {
		buffer.WriteString(", ")
		buffer.WriteString(fmt.Sprintf(" %s.data AS %s ", field, field))
	}
	buffer.WriteString(fmt.Sprintf(" FROM %s ", tableName))

	// 构建Join
	for _, field := range qb.fields {
		buffer.WriteString(fmt.Sprintf(" LEFT JOIN %s ON %s.object_id = %s.object_id ", field, tableName, field))
	}
	queryStmt, err := qb.buildQuery()
	if err != nil {
		return
	}
	buffer.WriteString(queryStmt)
	sortStmt, err := qb.buildSort()
	if err != nil {
		return
	}
	buffer.WriteString(sortStmt)
	buffer.WriteString(fmt.Sprintf(" LIMIT %d OFFSET %d", qb.limit, qb.offset))
	stmt = buffer.String()
	return
}

func (q *QueryNode) BuildQuerySQL() (stmt string, err error) {
	switch q.Type {
	case Connection:
		stmt, err = q.BuildConnect()
	case Operation:
		stmt, err = q.BuildOp()
	default:
		err = fmt.Errorf("unsupport queryNode type: %s from", q.Type)
	}
	return
}

func (q *QueryNode) BuildConnect() (stmt string, err error) {
	queryStmtList := []string{}

	for _, childQuery := range q.ChildNodes {
		var childStmt string
		childStmt, err = childQuery.BuildQuerySQL()
		if err != nil {
			return
		}
		queryStmtList = append(queryStmtList, childStmt)
	}
	switch q.Connect {
	case "and":
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
	case "or":
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
	default:
		err = fmt.Errorf("unsupport query connect type of %s", q.Connect)
	}
	return
}

func (q *QueryNode) BuildOp() (stmt string, err error) {
	stmt, err = q.QueryField.BuildQuery(q.QueryValue)
	return
}
