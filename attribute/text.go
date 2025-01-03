package attribute

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"paroket/object"
	"time"

	"github.com/google/uuid"
)

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

func (t *TextAttribute) GetField() string {
	return fieldText
}

func (t *TextAttribute) GetId() AttributeId {
	return t.id
}

func (t *TextAttribute) GetJSON() string {
	return fmt.Sprintf(`{"type": "%s", "value": "%s"}`, AttributeTypeText, t.value)
}

func (t *TextAttribute) GetType() string {
	return AttributeTypeText
}

func (t *TextAttribute) GetDataTableName() string {
	return getTableName(AttributeTypeText, t.classId)
}

func (t *TextAttribute) GetDataIndexName() string {
	return getIndexName(AttributeTypeText, t.classId)
}

func (t *TextAttribute) GetClassId() AttributeClassId {
	return t.classId
}

func (t *TextAttribute) InsertData(tx *sql.Tx, objId object.ObjectId) (err error) {
	insertAttributeStmt := fmt.Sprintf(`
	INSERT INTO %s
		(%s) 
	VALUES 
		(?, ?, ?, ?)`,
		t.GetDataTableName(), fieldText)
	_, err = tx.Exec(insertAttributeStmt, t.id, objId, time.Now(), t.GetJSON())

	if err != nil {
		return
	}

	insertIndexStmt := fmt.Sprintf(`
	INSERT INTO %s 
		(attribute_id, idx) 
	VALUES (?, ?)`, t.GetDataIndexName())

	_, err = tx.Exec(insertIndexStmt, t.id, t.value)
	return
}

func (t *TextAttribute) SearchData(tx *sql.Tx, objId object.ObjectId) (err error) {
	queryStmt := fmt.Sprintf(`SELECT %s FROM %s WHERE object_id = ?`, fieldText, t.GetDataTableName())
	err = t.ScanRow(tx.QueryRow(queryStmt, objId))
	return
}

func (t *TextAttribute) ScanRow(row *sql.Row) (err error) {
	var date time.Time
	var objId uuid.UUID
	var value string
	err = row.Scan(&t.id, &objId, &date, &value)
	jsonData := TextJsonData{}
	err = json.Unmarshal([]byte(value), &jsonData)
	t.value = jsonData.Value
	return
}

func (t *TextAttribute) ScanRows(rows *sql.Rows) (err error) {
	var date time.Time
	var objId uuid.UUID
	var value string
	err = rows.Scan(&t.id, &objId, &date, &value)
	jsonData := TextJsonData{}
	err = json.Unmarshal([]byte(value), &jsonData)
	t.value = jsonData.Value
	return
}

func (t *TextAttribute) UpdateData(tx *sql.Tx) (err error) {
	// 更新数据表
	updateAttributeStmt := fmt.Sprintf(`UPDATE %s SET update_time = ?, data = ? WHERE attribute_id = ?`, t.GetDataTableName())

	_, err = tx.Exec(updateAttributeStmt, time.Now(), t.GetJSON(), t.id)

	if err != nil {
		return
	}
	// 更新索引表
	updateIndexStmt := fmt.Sprintf(`UPDATE %s SET idx = ? WHERE attribute_id = ?`, t.GetDataIndexName())

	_, err = tx.Exec(updateIndexStmt, t.value, t.id)
	if err != nil {
		return
	}
	return
}

func (t *TextAttribute) DeleteData(tx *sql.Tx) (err error) {
	deleteIndexStmt := fmt.Sprintf(`DELETE FROM %s WHERE attribute_id = ?`, t.GetDataIndexName())
	_, err = tx.Exec(deleteIndexStmt, t.id)
	if err != nil {
		return
	}

	deleteAttributeStmt := fmt.Sprintf(`DELETE FROM %s WHERE attribute_id = ?`, t.GetDataTableName())
	_, err = tx.Exec(deleteAttributeStmt, t.id)

	if err != nil {
		return
	}

	return
}

func createTextTable(dataTableName, indexTableName string) (dataTable, indexTable, execIndex string) {
	dataTable = fmt.Sprintf(
		`CREATE TABLE %s (
		attribute_id BLOB PRIMARY KEY,
		object_id BLOB NOT NULL,
		update_time DATETIME NOT NULL,
		data JSONB NOT NULL,
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
		)`, dataTableName)

	indexTable = fmt.Sprintf(`CREATE TABLE %s (
		attribute_id BLOB NOT NULL,
		idx TEXT NOT NULL,
		FOREIGN KEY (attribute_id) REFERENCES %s(attribute_id) ON DELETE CASCADE
		)`, indexTableName, dataTableName)
	execIndex = fmt.Sprintf(`CREATE INDEX idx_%s ON %s(idx)`, indexTableName, indexTableName)
	return
}
