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

func (tc *TextAttributeClass) FromObject(obj common.Object) (attr common.Attribute, err error) {
	attrText := &TextAttribute{
		class: tc,
		value: "",
	}
	attr = attrText

	data := obj.Data()
	attrPath := fmt.Sprintf(`%v`, tc.id)
	attrData := gjson.Get(string(data), attrPath)
	if attrData.Type == gjson.Null {
		return
	}

	if err = attrText.Parse(attrData.Raw); err != nil {
		return
	}

	return
}

// 构建查询
func (tc *TextAttributeClass) BuildQuery(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (stmt string, err error) {
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

	jsonPath, ok := tc.metaInfo["json_value_path"].(string)
	if !ok {
		err = fmt.Errorf("TextAttribute metainfo dont have json_value_path")
		return
	}
	switch op {
	case "like":
		stmt = fmt.Sprintf(
			`(data ->> '%s' LIKE '%s%%' OR data ->> '%s' LIKE '%%%s%%')`,
			jsonPath,
			value,
			jsonPath,
			value,
		)
	case "unlike":
		stmt = fmt.Sprintf(
			`(data ->> '%s' NOT LIKE '%%%s%%')`,
			jsonPath,
			value,
		)
	case "equal":
		stmt = fmt.Sprintf(
			`(data ->> '%s' = '%s')`,
			jsonPath,
			value,
		)
	case "unequal":
		stmt = fmt.Sprintf(
			`(data ->> '%s' != '%s')`,
			jsonPath,
			value,
		)
	default:
		err = fmt.Errorf("unsupport op:%s", op)
	}
	return
}

// 构建排序
func (tc *TextAttributeClass) BuildSort(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (stmt string, err error) {
	if _, ok := v["mode"].(string); !ok {
		err = fmt.Errorf("invaild sort value:%s", v)
		return
	}
	mode := v["mode"].(string)
	jsonPath, ok := tc.metaInfo["json_value_path"].(string)
	if !ok {
		err = fmt.Errorf("TextAttribute metainfo dont have json_value_path")
		return
	}
	switch mode {
	case "asc":
		stmt = fmt.Sprintf("data ->> '%s' ASC", jsonPath)
	case "desc":
		stmt = fmt.Sprintf("data ->> '%s' DESC", jsonPath)

	}
	return
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
