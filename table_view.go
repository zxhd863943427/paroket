package paroket

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"paroket/common"
	"paroket/tx"

	"github.com/tidwall/gjson"
)

// TODO
// 删除表格的attribute貌似会对view造成影响，但现在还是懒得做了。

type viewImpl struct {
	viewId    common.ViewId
	db        common.Database
	table     common.Table
	fields    []common.AttributeClassId
	depFields []common.AttributeClassId
	filter    string
	order     string
	limit     int
	offset    int
}

func newView(_ context.Context, tx tx.WriteTx, db common.Database, table common.Table) (view common.View, err error) {

	fields := []common.AttributeClassId{}
	queryFields := `
	SELECT class_id 
	FROM table_to_attribute_classes 
	WHERE table_id = ?`
	rows, err := tx.Query(queryFields, table.TableId())
	if err != nil && err != sql.ErrNoRows {
		return
	}
	if err == nil {
		for rows.Next() {
			var field common.AttributeClassId
			if err = rows.Scan(&field); err != nil {
				return
			}
			fields = append(fields, field)
		}
	}
	id, err := common.NewViewId()
	if err != nil {
		return
	}
	view = &viewImpl{
		viewId: id,
		db:     db,
		table:  table,
		fields: fields,
		filter: "{}",
		order:  "[]",
		limit:  100,
		offset: 0,
	}

	insertView := `INSERT INTO 
	table_views (table_id, view_id, query)
	VALUES
	(?,?,?)
	`
	if _, err = tx.Exac(insertView, table.TableId(), view.ViewId(), view.Marshal()); err != nil {
		return
	}
	return
}

func queryView(_ context.Context, tx tx.ReadTx, db common.Database, table common.Table, vid common.ViewId) (view common.View, err error) {
	v := &viewImpl{
		viewId: vid,
		db:     db,
		table:  table,
		limit:  100,
		offset: 0,
	}
	queryStmt := ` 
	SELECT query FROM table_views WHERE view_id = ?`
	var data string
	if err = tx.QueryRow(queryStmt, vid).Scan(&data); err != nil {
		return
	}
	if err = v.Unmarshal(data); err != nil {
		return
	}
	view = v
	return
}
func (v *viewImpl) ViewId() common.ViewId {
	return v.viewId
}

func (v *viewImpl) Filter(tx tx.WriteTx, filter string) (err error) {
	if !gjson.Valid(filter) {
		return fmt.Errorf("invaild filter")
	}
	v.filter = filter
	if err = v.save(tx); err != nil {
		return
	}
	return
}
func (v *viewImpl) SortBy(tx tx.WriteTx, order string) (err error) {
	if !gjson.Valid(order) {
		return fmt.Errorf("invaild order")
	}
	v.order = order
	if err = v.save(tx); err != nil {
		return
	}
	return
}
func (v *viewImpl) Limit(limit int) common.View {
	v.limit = limit
	return v
}
func (v *viewImpl) Offset(offset int) common.View {
	v.offset = offset
	return v
}
func (v *viewImpl) Set(value map[string]interface{}) (err error) {
	// TODO
	panic("un impl")
}

func (v *viewImpl) save(tx tx.WriteTx) (err error) {
	update := `UPDATE table_views SET query = ? WHERE view_id = ?`
	if _, err = tx.Exac(update, v.Marshal(), v.viewId); err != nil {
		return
	}

	return
}

func (v *viewImpl) Query(ctx context.Context, tx tx.ReadTx) (queryData common.TableResult, err error) {
	query := NewQueryBuilder(v.table, v.db)
	if err = query.ParseFilter(ctx, tx, v.filter); err != nil {
		return
	}
	if err = query.ParseOrder(ctx, tx, v.order); err != nil {
		return
	}

	queryStmtBuffer := &bytes.Buffer{}
	queryStmtBuffer.WriteString(fmt.Sprintf(`
	SELECT object_id, json(data) FROM %s `, v.table.TableId().DataTable()))

	filterStmt, err := query.BuildFilter(ctx, tx)
	if err != nil {
		return
	}
	orderStmt, err := query.BuildSort(ctx, tx)
	if err != nil {
		return
	}
	queryStmtBuffer.WriteString(fmt.Sprintf(`
	%s
	%s
	LIMIT %d OFFSET %d`, filterStmt, orderStmt, v.limit, v.offset))

	queryStmt := queryStmtBuffer.String()
	rows, err := tx.Query(queryStmt)
	if err != nil {
		return
	}
	objList, err := common.QueryTableObject(ctx, rows)
	if err != nil {
		return
	}
	queryData = common.NewTableResult(v.db, v.fields, objList)
	return
}

func marshalAcIdList(acidList []common.AttributeClassId) string {
	listBuffer := &bytes.Buffer{}
	listBuffer.WriteString("[")
	for idx, field := range acidList {
		if idx != 0 {
			listBuffer.WriteString(",")
		}
		listBuffer.WriteString(fmt.Sprintf(`"%v"`, field))
	}
	listBuffer.WriteString("]")
	ret := listBuffer.String()
	return ret
}

func (v *viewImpl) Marshal() string {

	fieldMashal := marshalAcIdList(v.fields)
	depFieldMashal := marshalAcIdList(v.depFields)

	return fmt.Sprintf(`{"fields":%s,"dep_fields":%s,"filter":%s,"order":%s}`,
		fieldMashal, depFieldMashal, v.filter, v.order,
	)
}

func (v *viewImpl) Unmarshal(data string) (err error) {
	fieldsData := gjson.Get(data, "field")
	if fieldsData.Type != gjson.JSON {
		return fmt.Errorf("field unfound")
	}
	depFieldData := gjson.Get(data, "dep_fields")
	if depFieldData.Type != gjson.JSON {
		return fmt.Errorf("depField unfound")
	}
	filterData := gjson.Get(data, "filter")
	if filterData.Type != gjson.JSON {
		return fmt.Errorf("field unfound")
	}

	orderData := gjson.Get(data, "order")
	if orderData.Type != gjson.JSON {
		return fmt.Errorf("order unfound")
	}
	field := []common.AttributeClassId{}
	fieldsData.ForEach(func(key, value gjson.Result) bool {
		acid := common.AttributeClassId{}
		err = acid.Scan(value.String())
		if err != nil {
			return false
		}
		field = append(field, acid)
		return true // keep iterating
	})

	depFields := []common.AttributeClassId{}
	depFieldData.ForEach(func(key, value gjson.Result) bool {
		acid := common.AttributeClassId{}
		err = acid.Scan(value.String())
		if err != nil {
			return false
		}
		depFields = append(depFields, acid)
		return true // keep iterating
	})
	v.fields = field
	v.depFields = depFields
	v.filter = filterData.Raw
	v.order = orderData.Raw
	return
}
