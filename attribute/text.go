package attribute

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"paroket/object"
	"time"

	"github.com/rs/xid"
)

type TextAttributeClass struct {
	AttributeClass AttributeClass
}

type TextAttribute struct {
	id      AttributeId
	classId AttributeClassId
	value   string
}

type TextJsonData struct {
	Type  string
	Value string
}

const fieldText = ` attribute_id, object_id, update_time, data `

func (tc *TextAttributeClass) NewAttribute() (attr Attribute, err error) {
	id, err := NewAttributeId()
	if err != nil {
		return
	}
	attr = &TextAttribute{
		id:      id,
		classId: tc.AttributeClass.ClassId,
		value:   "",
	}
	return
}

func (tc *TextAttributeClass) CreateDataTable(tx *sql.Tx) (err error) {
	// dataTableName := tc.GetDataTableName()
	// indexTableName := tc.GetDataIndexName()
	// dataTable := fmt.Sprintf(
	// 	`CREATE TABLE %s (
	// 	attribute_id BLOB PRIMARY KEY,
	// 	object_id BLOB UNIQUE NOT NULL,
	// 	update_time DATETIME NOT NULL,
	// 	data JSONB NOT NULL,
	// 	FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
	// 	)`, dataTableName)

	// indexTable := fmt.Sprintf(`CREATE TABLE %s (
	// 	attribute_id BLOB NOT NULL,
	// 	idx TEXT NOT NULL,
	// 	FOREIGN KEY (attribute_id) REFERENCES %s(attribute_id) ON DELETE CASCADE
	// 	)`, indexTableName, dataTableName)
	// execIndex := fmt.Sprintf(`
	// CREATE INDEX idx_%s ON %s(idx, attribute_id);
	// CREATE INDEX idx_%s_data ON %s(object_id, data);
	// CREATE INDEX idx_%s_data_sort ON %s(data  -> '$.value');
	// `,
	// 	indexTableName, indexTableName,
	// 	dataTableName, dataTableName,
	// 	dataTableName, dataTableName,
	// )
	return
}

func (tc *TextAttributeClass) SearchByID(tx *sql.Tx, objId object.ObjectId) (attr Attribute, err error) {
	t := &TextAttribute{classId: tc.AttributeClass.ClassId}
	attr = t
	queryStmt := fmt.Sprintf(`SELECT %s FROM %s WHERE object_id = ?`, fieldText, tc.GetDataTableName())
	err = t.ScanRow(tx.QueryRow(queryStmt, objId))
	return
}

func (tc *TextAttributeClass) GetDataTableName() string {
	return fmt.Sprintf(
		"%s_%s",
		AttributeTypeText,
		tc.AttributeClass.ClassId.String())
}

func (tc *TextAttributeClass) GetDataIndexName() string {
	return fmt.Sprintf(
		`%s_%s_idx`,
		AttributeTypeText,
		tc.AttributeClass.ClassId.String())
}

func (tc *TextAttributeClass) BuildQuery(v map[string]interface{}) (stmt string, err error) {
	if _, ok := v["op"].(string); !ok {
		err = fmt.Errorf("invaild query value:%s", v)
		return
	}
	if _, ok := v["value"].(string); !ok {
		err = fmt.Errorf("invaild query value:%s", v)
		return
	}
	op := v["op"].(string)
	value := v["value"].(string)
	switch op {
	case "like":
		stmt = fmt.Sprintf(
			`( %s.attribute_id in ( 
			SELECT attribute_id FROM %s 
			WHERE %s.idx LIKE '%s%%' OR %s.idx LIKE '%%%s%%'
			))`,
			tc.GetDataTableName(),
			tc.GetDataIndexName(),
			tc.GetDataIndexName(),
			value,
			tc.GetDataIndexName(),
			value,
		)
	case "unlike":
		stmt = fmt.Sprintf(
			`(%s.attribute_id in ( 
			SELECT attribute_id FROM %s 
			WHERE %s.idx NOT LIKE '%%%s%%'
			))`,
			tc.GetDataTableName(),
			tc.GetDataIndexName(),
			tc.GetDataIndexName(),
			value,
		)
	case "equal":
		stmt = fmt.Sprintf(
			`(%s.attribute_id in ( 
			SELECT attribute_id FROM %s 
			WHERE %s.idx == '%s'
			))`,
			tc.GetDataTableName(),
			tc.GetDataIndexName(),
			tc.GetDataIndexName(),
			value,
		)
	case "unequal":
		stmt = fmt.Sprintf(
			`(%s.attribute_id in ( 
			SELECT attribute_id FROM %s 
			WHERE %s.idx != '%s'
			))`,
			tc.GetDataTableName(),
			tc.GetDataIndexName(),
			tc.GetDataIndexName(),
			value,
		)
	default:
		err = fmt.Errorf("unsupport op:%s", op)
	}
	return
}

func (tc *TextAttributeClass) BuildSort(v map[string]interface{}) (stmt string, err error) {
	//TODO
	if _, ok := v["mode"].(string); !ok {
		err = fmt.Errorf("invaild sort value:%s", v)
		return
	}
	mode := v["mode"].(string)
	switch mode {
	case "asc":
		stmt = fmt.Sprintf("%s.data -> '$.value' ASC", tc.GetDataTableName())
	case "desc":
		stmt = fmt.Sprintf("%s.data -> '$.value' DESC", tc.GetDataTableName())

	}
	return
}

func (t *TextAttribute) GetField() string {
	return fieldText
}

func (t *TextAttribute) GetId() AttributeId {
	return t.id
}

func (t *TextAttribute) GetJSON() string {
	return fmt.Sprintf(`{"type": "%s", "value": "%s"}`, AttributeTypeText, t.value)
}

func (t *TextAttribute) String() string {
	return t.value
}

func (t *TextAttribute) GetType() string {
	return AttributeTypeText
}

func (t *TextAttribute) GetTableName() string {
	return genTableNameByTypeAndID(AttributeTypeText, t.classId)
}

func (t *TextAttribute) GetIndexName() string {
	return genIndexNameByTypeAndID(AttributeTypeText, t.classId)
}

func (t *TextAttribute) GetClassId() AttributeClassId {
	return t.classId
}

func (t *TextAttribute) SetValue(v map[string]interface{}) (err error) {
	if str, ok := v["value"].(string); ok {
		t.value = str
	} else {
		err = fmt.Errorf("set TextAttribute failed with error type: %T", v["value"])
	}
	return
}

func (t *TextAttribute) InsertData(tx *sql.Tx, objId object.ObjectId) (err error) {
	insertAttributeStmt := fmt.Sprintf(`
	INSERT INTO %s
		(%s) 
	VALUES 
		(?, ?, ?, ?)`,
		t.GetTableName(), fieldText)
	_, err = tx.Exec(insertAttributeStmt, t.id, objId, time.Now(), t.GetJSON())

	if err != nil {
		return
	}

	insertIndexStmt := fmt.Sprintf(`
	INSERT INTO %s 
		(attribute_id, idx) 
	VALUES (?, ?)`, t.GetIndexName())

	_, err = tx.Exec(insertIndexStmt, t.id, t.value)
	return
}

func (t *TextAttribute) ScanRow(row *sql.Row) (err error) {
	var date time.Time
	var objId xid.ID
	var value string
	err = row.Scan(&t.id, &objId, &date, &value)
	if err != nil {
		return
	}
	jsonData := TextJsonData{}
	err = json.Unmarshal([]byte(value), &jsonData)
	t.value = jsonData.Value
	return
}

func (t *TextAttribute) ScanRows(rows *sql.Rows) (err error) {
	var date time.Time
	var objId xid.ID
	var value string
	err = rows.Scan(&t.id, &objId, &date, &value)
	if err != nil {
		return
	}
	jsonData := TextJsonData{}
	err = json.Unmarshal([]byte(value), &jsonData)
	t.value = jsonData.Value
	return
}

func (t *TextAttribute) UpdateData(tx *sql.Tx) (err error) {
	// 更新数据表
	updateAttributeStmt := fmt.Sprintf(`UPDATE %s SET update_time = ?, data = ? WHERE attribute_id = ?`, t.GetTableName())

	_, err = tx.Exec(updateAttributeStmt, time.Now(), t.GetJSON(), t.id)

	if err != nil {
		return
	}
	// 更新索引表
	updateIndexStmt := fmt.Sprintf(`UPDATE %s SET idx = ? WHERE attribute_id = ?`, t.GetIndexName())

	_, err = tx.Exec(updateIndexStmt, t.value, t.id)
	if err != nil {
		return
	}
	return
}

func (t *TextAttribute) DeleteData(tx *sql.Tx) (err error) {
	deleteIndexStmt := fmt.Sprintf(`DELETE FROM %s WHERE attribute_id = ?`, t.GetIndexName())
	_, err = tx.Exec(deleteIndexStmt, t.id)
	if err != nil {
		return
	}

	deleteAttributeStmt := fmt.Sprintf(`DELETE FROM %s WHERE attribute_id = ?`, t.GetTableName())
	_, err = tx.Exec(deleteAttributeStmt, t.id)

	if err != nil {
		return
	}

	return
}
