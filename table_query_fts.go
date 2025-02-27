package paroket

import (
	"bytes"
	"context"
	"fmt"
	"paroket/common"
	"paroket/tx"
	"strings"
)

type ftsFilterField struct{}

func newFts() common.FilterField {
	return &ftsFilterField{}
}
func (f *ftsFilterField) BuildQuery(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (stmt string, err error) {
	op, ok := v["op"].(string)
	if !ok {
		err = fmt.Errorf("invaild query value:%s", v)
		return
	}
	val, ok := v["value"].(string)
	if !ok {
		err = fmt.Errorf("invaild query value:%s", v)
		return
	}
	searchKeys := strings.Split(val, " ")
	stmtBuffer := &bytes.Buffer{}
	stmtBuffer.WriteString(" (")
	switch op {
	case "search":
		for idx, key := range searchKeys {
			if idx != 0 {
				stmtBuffer.WriteString(" AND ")
			}
			searchStmt := fmt.Sprintf(`idx like '%%%s%%'`, key)
			stmtBuffer.WriteString(searchStmt)
		}
	default:
		err = fmt.Errorf("fts unsupport op type")
		return
	}
	stmtBuffer.WriteString(") ")
	stmt = stmtBuffer.String()
	return
}
