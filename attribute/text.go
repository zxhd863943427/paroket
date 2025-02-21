package attribute

import (
	"context"
	"database/sql"
	"fmt"
	"paroket/common"
	"paroket/tx"
	"paroket/utils"

	"github.com/rs/xid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type TextAttributeClass struct {
	attributeClass
}

type TextAttribute struct {
	class *TextAttributeClass
	value string
}

type TextJsonData struct {
	Type  string
	Value string
}

func newTextAttributeClass(_ context.Context, db common.Database, tx tx.WriteTx) (ac common.AttributeClass, err error) {

	id, err := common.NewAttributeClassId()
	if err != nil {
		return
	}
	jsonValuePath := fmt.Sprintf(`$."%v"."value"`, id)
	updateTable := fmt.Sprintf(`text_%v`, id)

	act := &TextAttributeClass{
		attributeClass{
			db:       db,
			id:       id,
			name:     "text",
			key:      id.String(),
			attrType: AttributeTypeText,
			metaInfo: utils.JSONMap{
				"json_value_path": jsonValuePath,
				"updated_table":   updateTable,
			},
		},
	}
	ac = act

	stmt := `
  INSERT INTO attribute_classes
  (class_id,attribute_name,attribute_key,attribute_type,attribute_meta_info)
  VALUES
  (?,?,?,?,?)`
	createUpdate := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %v(
    object_id BLOB PRIMARY KEY,
    updated BLOB NOT NULL,
	FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
)`, updateTable)
	if _, err = tx.Exac(stmt, act.id, act.name, act.key, act.attrType, act.metaInfo); err != nil {
		return
	}
	if _, err = tx.Exac(createUpdate); err != nil {
		return
	}

	return
}

func parseTextAttributeClass(_ context.Context, acProto *attributeClass) (ac common.AttributeClass, err error) {
	ac = &TextAttributeClass{*acProto}
	return
}

func (tc *TextAttributeClass) Name() string {
	return tc.name
}
func (tc *TextAttributeClass) Type() common.AttributeType {
	return tc.attrType
}
func (tc *TextAttributeClass) ClassId() common.AttributeClassId {
	return tc.id
}

func (tc *TextAttributeClass) GetMetaInfo(ctx context.Context, tx tx.ReadTx) (v utils.JSONMap, err error) {
	m := utils.JSONMap{}
	for key := range tc.metaInfo {
		m[key] = tc.metaInfo[key]
	}
	return m, nil
}
func (tc *TextAttributeClass) Set(ctx context.Context, tx tx.WriteTx, v utils.JSONMap) (err error) {
	oldName := tc.name
	oldkey := tc.key
	oldMetaInfo := utils.JSONMap{}
	for key := range tc.metaInfo {
		oldMetaInfo[key] = tc.metaInfo[key]
	}
	defer func() {
		if err != nil {
			tc.name = oldName
			tc.key = oldkey
			tc.metaInfo = oldMetaInfo
		}
	}()

	if name, ok := v["name"]; ok {
		switch value := name.(type) {
		case string:
			tc.name = value
		default:
			err = fmt.Errorf("set name with error type")
			return
		}
		delete(v, "name")
	}

	if key, ok := v["key"]; ok {
		switch value := key.(type) {
		case string:
			tc.key = value
		default:
			err = fmt.Errorf("set key with error type")
			return
		}
		delete(v, "key")
	}
	for key := range v {
		tc.metaInfo[key] = v[key]
	}
	stmt := `
  UPDATE attribute_classes
  SET (attribute_name,attribute_key,attribute_meta_info) = 
  (?,?,?)
  WHERE class_id = ?`
	if _, err = tx.Exac(stmt, tc.name, tc.key, tc.metaInfo, tc.id); err != nil {
		return
	}
	return
}

func (tc *TextAttributeClass) Insert(ctx context.Context, tx tx.WriteTx, oid common.ObjectId) (attr common.Attribute, err error) {

	attrText := &TextAttribute{
		class: tc,
		value: "",
	}
	attr = attrText
	obj, err := tc.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	data := obj.Data()
	newValue, err := sjson.SetRaw(string(data), tc.id.String(), attr.GetJSON())
	if err != nil {
		return
	}
	err = obj.Update(ctx, tx, []byte(newValue))
	if err != nil {
		return
	}

	updateTable, ok := tc.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("TextAttribute metainfo dont have updated_table")
		return
	}
	update := fmt.Sprintf(`
INSERT INTO %s
  (object_id, updated)
VALUES 
  (?,?)`, updateTable)
	opId := xid.New()

	_, err = tx.Exac(update, oid, opId)
	if err != nil {
		return
	}
	return

}
func (tc *TextAttributeClass) FindId(ctx context.Context, tx tx.ReadTx, oid common.ObjectId) (attr common.Attribute, err error) {
	obj, err := tc.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	data := obj.Data()
	attrPath := fmt.Sprintf(`%v`, tc.id)
	attrData := gjson.Get(string(data), attrPath)
	if attrData.Type == gjson.Null {
		err = sql.ErrNoRows
		return
	}
	attrText := &TextAttribute{
		class: tc,
		value: "",
	}
	if err = attrText.Parse(attrData.Raw); err != nil {
		return
	}
	attr = attrText
	return
}
func (tc *TextAttributeClass) Update(ctx context.Context, tx tx.WriteTx, oid common.ObjectId, attr common.Attribute) (err error) {
	obj, err := tc.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	data := obj.Data()
	newValue, err := sjson.SetRaw(string(data), tc.id.String(), attr.GetJSON())
	if err != nil {
		return
	}
	err = obj.Update(ctx, tx, []byte(newValue))
	if err != nil {
		return
	}

	updateTable, ok := tc.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("TextAttribute metainfo dont have updated_table")
		return
	}
	update := fmt.Sprintf(`
UPDATE %s SET updated = ?
  WHERE object_id = ?
    `, updateTable)
	opId := xid.New()

	if _, err = tx.Exac(update, opId, oid); err != nil {
		return
	}

	return
}
func (tc *TextAttributeClass) Delete(ctx context.Context, tx tx.WriteTx, oid common.ObjectId) (err error) {
	obj, err := tc.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	data := obj.Data()
	newValue, err := sjson.Delete(string(data), tc.id.String())
	if err != nil {
		return
	}
	err = obj.Update(ctx, tx, []byte(newValue))
	if err != nil {
		return
	}
	updateTable, ok := tc.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("TextAttribute metainfo dont have updated_table")
		return
	}
	deleteRecord := fmt.Sprintf(`
DELETE FROM %s WHERE object_id = ?
`, updateTable)
	if _, err = tx.Exac(deleteRecord, oid); err != nil {
		return
	}
	return
}

func (tc *TextAttributeClass) Drop(ctx context.Context, tx tx.WriteTx) (err error) {

	updateTable, ok := tc.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("TextAttribute metainfo dont have updated_table")
		return
	}
	//先删除相关表的索引
	tidList := []common.TableId{}
	queryTableId := `
	SELECT table_id FROM table_to_attribute_classes WHERE class_id = ?`
	rows, err := tx.Query(queryTableId, tc.id)
	if err != nil {
		return
	}
	for rows.Next() {
		var tid common.TableId
		if err = rows.Scan(&tid); err != nil {
			return
		}
		tidList = append(tidList, tid)
	}
	for _, tid := range tidList {
		var table common.Table
		table, err = tc.db.OpenTable(ctx, tx, tid)
		if err != nil {
			return
		}
		table.DeleteAttributeClass(ctx, tx, tc)
	}

	// 从相关的object中移除attribute
	oidList := []common.ObjectId{}
	queryObjectId := fmt.Sprintf(`
	SELECT object_id FROM %s`, updateTable)
	rows, err = tx.Query(queryObjectId)
	if err != nil {
		return
	}
	for rows.Next() {
		var oid common.ObjectId
		if err = rows.Scan(&oid); err != nil {
			return
		}
		oidList = append(oidList, oid)
	}

	for _, oid := range oidList {
		var obj common.Object
		var newValue string
		obj, err = tc.db.OpenObject(ctx, tx, oid)
		if err != nil {
			return
		}
		data := obj.Data()
		newValue, err = sjson.Delete(string(data), tc.id.String())
		if err != nil {
			return
		}
		err = obj.Update(ctx, tx, []byte(newValue))
		if err != nil {
			return
		}
	}

	dropTable := fmt.Sprintf("DROP TABLE %s", updateTable)
	if _, err = tx.Exac(dropTable); err != nil {
		return
	}

	deleteAttributeClassStmt := `DELETE FROM attribute_classes WHERE class_id = ?`
	if _, err = tx.Exac(deleteAttributeClassStmt, tc.id); err != nil {
		return
	}
	return
}

// 构建查询
func (tc *TextAttributeClass) BuildQuery(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (string, error) {
	// TODO
	panic("un impl")
}

// 构建排序
func (tc *TextAttributeClass) BuildSort(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (string, error) {
	// TODO
	panic("un impl")
}

func (t *TextAttribute) GetJSON() string {
	return fmt.Sprintf(`{"value":"%s"}`, t.value)
}
func (t *TextAttribute) String() string {
	return t.value
}
func (t *TextAttribute) GetClass() common.AttributeClass {
	return t.class
}
func (t *TextAttribute) SetValue(v map[string]interface{}) (err error) {
	if value, ok := v["value"].(string); ok {
		t.value = value
		return
	}
	err = fmt.Errorf("invaild set value:%v", v)
	return
}
func (t *TextAttribute) Parse(v string) error {
	result := gjson.Get(v, "value")
	if result.Type != gjson.String {
		return fmt.Errorf("parse error: %v", v)
	}
	t.value = result.Str
	return nil
}

// func (tc *TextAttributeClass) NewAttribute() (attr Attribute, err error) {
// 	id, err := NewAttributeId()
// 	if err != nil {
// 		return
// 	}
// 	attr = &TextAttribute{
// 		id:      id,
// 		classId: tc.AttributeClass.ClassId,
// 		value:   "",
// 	}
// 	return
// }

// func (tc *TextAttributeClass) CreateDataTable(tx *sql.Tx) (err error) {
// 	// dataTableName := tc.GetDataTableName()
// 	// indexTableName := tc.GetDataIndexName()
// 	// dataTable := fmt.Sprintf(
// 	// 	`CREATE TABLE %s (
// 	// 	attribute_id BLOB PRIMARY KEY,
// 	// 	object_id BLOB UNIQUE NOT NULL,
// 	// 	update_time DATETIME NOT NULL,
// 	// 	data JSONB NOT NULL,
// 	// 	FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
// 	// 	)`, dataTableName)

// 	// indexTable := fmt.Sprintf(`CREATE TABLE %s (
// 	// 	attribute_id BLOB NOT NULL,
// 	// 	idx TEXT NOT NULL,
// 	// 	FOREIGN KEY (attribute_id) REFERENCES %s(attribute_id) ON DELETE CASCADE
// 	// 	)`, indexTableName, dataTableName)
// 	// execIndex := fmt.Sprintf(`
// 	// CREATE INDEX idx_%s ON %s(idx, attribute_id);
// 	// CREATE INDEX idx_%s_data ON %s(object_id, data);
// 	// CREATE INDEX idx_%s_data_sort ON %s(data  -> '$.value');
// 	// `,
// 	// 	indexTableName, indexTableName,
// 	// 	dataTableName, dataTableName,
// 	// 	dataTableName, dataTableName,
// 	// )
// 	return
// }

// func (tc *TextAttributeClass) SearchByID(tx *sql.Tx, objId object.ObjectId) (attr Attribute, err error) {
// 	t := &TextAttribute{classId: tc.AttributeClass.ClassId}
// 	attr = t
// 	queryStmt := fmt.Sprintf(`SELECT %s FROM %s WHERE object_id = ?`, fieldText, tc.GetDataTableName())
// 	err = t.ScanRow(tx.QueryRow(queryStmt, objId))
// 	return
// }

// func (tc *TextAttributeClass) GetDataTableName() string {
// 	return fmt.Sprintf(
// 		"%s_%s",
// 		AttributeTypeText,
// 		tc.AttributeClass.ClassId.String())
// }

// func (tc *TextAttributeClass) GetDataIndexName() string {
// 	return fmt.Sprintf(
// 		`%s_%s_idx`,
// 		AttributeTypeText,
// 		tc.AttributeClass.ClassId.String())
// }

// func (tc *TextAttributeClass) BuildQuery(v map[string]interface{}) (stmt string, err error) {
// 	if _, ok := v["op"].(string); !ok {
// 		err = fmt.Errorf("invaild query value:%s", v)
// 		return
// 	}
// 	if _, ok := v["value"].(string); !ok {
// 		err = fmt.Errorf("invaild query value:%s", v)
// 		return
// 	}
// 	op := v["op"].(string)
// 	value := v["value"].(string)
// 	switch op {
// 	case "like":
// 		stmt = fmt.Sprintf(
// 			`( %s.attribute_id in (
// 			SELECT attribute_id FROM %s
// 			WHERE %s.idx LIKE '%s%%' OR %s.idx LIKE '%%%s%%'
// 			))`,
// 			tc.GetDataTableName(),
// 			tc.GetDataIndexName(),
// 			tc.GetDataIndexName(),
// 			value,
// 			tc.GetDataIndexName(),
// 			value,
// 		)
// 	case "unlike":
// 		stmt = fmt.Sprintf(
// 			`(%s.attribute_id in (
// 			SELECT attribute_id FROM %s
// 			WHERE %s.idx NOT LIKE '%%%s%%'
// 			))`,
// 			tc.GetDataTableName(),
// 			tc.GetDataIndexName(),
// 			tc.GetDataIndexName(),
// 			value,
// 		)
// 	case "equal":
// 		stmt = fmt.Sprintf(
// 			`(%s.attribute_id in (
// 			SELECT attribute_id FROM %s
// 			WHERE %s.idx == '%s'
// 			))`,
// 			tc.GetDataTableName(),
// 			tc.GetDataIndexName(),
// 			tc.GetDataIndexName(),
// 			value,
// 		)
// 	case "unequal":
// 		stmt = fmt.Sprintf(
// 			`(%s.attribute_id in (
// 			SELECT attribute_id FROM %s
// 			WHERE %s.idx != '%s'
// 			))`,
// 			tc.GetDataTableName(),
// 			tc.GetDataIndexName(),
// 			tc.GetDataIndexName(),
// 			value,
// 		)
// 	default:
// 		err = fmt.Errorf("unsupport op:%s", op)
// 	}
// 	return
// }

// func (tc *TextAttributeClass) BuildSort(v map[string]interface{}) (stmt string, err error) {
// 	//TODO
// 	if _, ok := v["mode"].(string); !ok {
// 		err = fmt.Errorf("invaild sort value:%s", v)
// 		return
// 	}
// 	mode := v["mode"].(string)
// 	switch mode {
// 	case "asc":
// 		stmt = fmt.Sprintf("%s.data -> '$.value' ASC", tc.GetDataTableName())
// 	case "desc":
// 		stmt = fmt.Sprintf("%s.data -> '$.value' DESC", tc.GetDataTableName())

// 	}
// 	return
// }

// func (t *TextAttribute) GetField() string {
// 	return fieldText
// }

// func (t *TextAttribute) GetId() AttributeId {
// 	return t.id
// }

// func (t *TextAttribute) GetJSON() string {
// 	return fmt.Sprintf(`{"type": "%s", "value": "%s"}`, AttributeTypeText, t.value)
// }

// func (t *TextAttribute) String() string {
// 	return t.value
// }

// func (t *TextAttribute) GetType() string {
// 	return AttributeTypeText
// }

// func (t *TextAttribute) GetTableName() string {
// 	return genTableNameByTypeAndID(AttributeTypeText, t.classId)
// }

// func (t *TextAttribute) GetIndexName() string {
// 	return genIndexNameByTypeAndID(AttributeTypeText, t.classId)
// }

// func (t *TextAttribute) GetClassId() AttributeClassId {
// 	return t.classId
// }

// func (t *TextAttribute) SetValue(v map[string]interface{}) (err error) {
// 	if str, ok := v["value"].(string); ok {
// 		t.value = str
// 	} else {
// 		err = fmt.Errorf("set TextAttribute failed with error type: %T", v["value"])
// 	}
// 	return
// }

// func (t *TextAttribute) InsertData(tx *sql.Tx, objId object.ObjectId) (err error) {
// 	insertAttributeStmt := fmt.Sprintf(`
// 	INSERT INTO %s
// 		(%s)
// 	VALUES
// 		(?, ?, ?, ?)`,
// 		t.GetTableName(), fieldText)
// 	_, err = tx.Exec(insertAttributeStmt, t.id, objId, time.Now(), t.GetJSON())

// 	if err != nil {
// 		return
// 	}

// 	insertIndexStmt := fmt.Sprintf(`
// 	INSERT INTO %s
// 		(attribute_id, idx)
// 	VALUES (?, ?)`, t.GetIndexName())

// 	_, err = tx.Exec(insertIndexStmt, t.id, t.value)
// 	return
// }

// func (t *TextAttribute) ScanRow(row *sql.Row) (err error) {
// 	var date time.Time
// 	var objId xid.ID
// 	var value string
// 	err = row.Scan(&t.id, &objId, &date, &value)
// 	if err != nil {
// 		return
// 	}
// 	jsonData := TextJsonData{}
// 	err = json.Unmarshal([]byte(value), &jsonData)
// 	t.value = jsonData.Value
// 	return
// }

// func (t *TextAttribute) ScanRows(rows *sql.Rows) (err error) {
// 	var date time.Time
// 	var objId xid.ID
// 	var value string
// 	err = rows.Scan(&t.id, &objId, &date, &value)
// 	if err != nil {
// 		return
// 	}
// 	jsonData := TextJsonData{}
// 	err = json.Unmarshal([]byte(value), &jsonData)
// 	t.value = jsonData.Value
// 	return
// }

// func (t *TextAttribute) UpdateData(tx *sql.Tx) (err error) {
// 	// 更新数据表
// 	updateAttributeStmt := fmt.Sprintf(`UPDATE %s SET update_time = ?, data = ? WHERE attribute_id = ?`, t.GetTableName())

// 	_, err = tx.Exec(updateAttributeStmt, time.Now(), t.GetJSON(), t.id)

// 	if err != nil {
// 		return
// 	}
// 	// 更新索引表
// 	updateIndexStmt := fmt.Sprintf(`UPDATE %s SET idx = ? WHERE attribute_id = ?`, t.GetIndexName())

// 	_, err = tx.Exec(updateIndexStmt, t.value, t.id)
// 	if err != nil {
// 		return
// 	}
// 	return
// }

// func (t *TextAttribute) DeleteData(tx *sql.Tx) (err error) {
// 	deleteIndexStmt := fmt.Sprintf(`DELETE FROM %s WHERE attribute_id = ?`, t.GetIndexName())
// 	_, err = tx.Exec(deleteIndexStmt, t.id)
// 	if err != nil {
// 		return
// 	}

// 	deleteAttributeStmt := fmt.Sprintf(`DELETE FROM %s WHERE attribute_id = ?`, t.GetTableName())
// 	_, err = tx.Exec(deleteAttributeStmt, t.id)

// 	if err != nil {
// 		return
// 	}

// 	return
// }
