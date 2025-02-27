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

type NumberAttributeClass struct {
	AttributeClassInfo
}

type NumberAttribute struct {
	class *NumberAttributeClass
	value float64
}

func newNumberAttributeClass(_ context.Context, db common.Database, tx tx.WriteTx) (ac common.AttributeClass, err error) {

	id, err := common.NewAttributeClassId()
	if err != nil {
		return
	}
	jsonValuePath := fmt.Sprintf(`$."%v"."value"`, id)
	updateTable := fmt.Sprintf(`number_%v`, id)

	act := &NumberAttributeClass{
		AttributeClassInfo{
			db:       db,
			id:       id,
			name:     "number",
			key:      id.String(),
			attrType: AttributeTypeNumber,
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

func parseNumberAttributeClass(_ context.Context, acProto *AttributeClassInfo) (ac common.AttributeClass, err error) {
	ac = &NumberAttributeClass{*acProto}
	return
}

func (tc *NumberAttributeClass) GetMetaInfo(ctx context.Context, tx tx.ReadTx) (v utils.JSONMap, err error) {
	m := utils.JSONMap{}
	for key := range tc.metaInfo {
		m[key] = tc.metaInfo[key]
	}
	return m, nil
}
func (tn *NumberAttributeClass) Set(ctx context.Context, tx tx.WriteTx, v utils.JSONMap) (err error) {
	oldName := tn.name
	oldkey := tn.key
	oldMetaInfo := utils.JSONMap{}
	for key := range tn.metaInfo {
		oldMetaInfo[key] = tn.metaInfo[key]
	}
	defer func() {
		if err != nil {
			tn.name = oldName
			tn.key = oldkey
			tn.metaInfo = oldMetaInfo
		}
	}()

	if name, ok := v["name"]; ok {
		switch value := name.(type) {
		case string:
			tn.name = value
		default:
			err = fmt.Errorf("set name with error type")
			return
		}
		delete(v, "name")
	}

	if key, ok := v["key"]; ok {
		switch value := key.(type) {
		case string:
			tn.key = value
		default:
			err = fmt.Errorf("set key with error type")
			return
		}
		delete(v, "key")
	}
	for key := range v {
		tn.metaInfo[key] = v[key]
	}
	stmt := `
  UPDATE attribute_classes
  SET (attribute_name,attribute_key,attribute_meta_info) = 
  (?,?,?)
  WHERE class_id = ?`
	if _, err = tx.Exac(stmt, tn.name, tn.key, tn.metaInfo, tn.id); err != nil {
		return
	}
	return
}

func (tn *NumberAttributeClass) Insert(ctx context.Context, tx tx.WriteTx, oid common.ObjectId) (attr common.Attribute, err error) {

	attrText := &NumberAttribute{
		class: tn,
		value: 0,
	}
	attr = attrText
	obj, err := tn.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	data := obj.Data()
	newValue, err := sjson.SetRaw(string(data), tn.id.String(), attr.GetJSON())
	if err != nil {
		return
	}
	err = obj.Update(ctx, tx, []byte(newValue))
	if err != nil {
		return
	}

	updateTable, ok := tn.metaInfo["updated_table"].(string)
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
func (tn *NumberAttributeClass) FindId(ctx context.Context, tx tx.ReadTx, oid common.ObjectId) (attr common.Attribute, err error) {
	obj, err := tn.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	data := obj.Data()
	attrPath := fmt.Sprintf(`%v`, tn.id)
	attrData := gjson.Get(string(data), attrPath)
	if attrData.Type == gjson.Null {
		err = sql.ErrNoRows
		return
	}
	attrNum := &NumberAttribute{
		class: tn,
		value: 0,
	}
	if err = attrNum.Parse(attrData.Raw); err != nil {
		return
	}
	attr = attrNum
	return
}
func (tn *NumberAttributeClass) Update(ctx context.Context, tx tx.WriteTx, oid common.ObjectId, attr common.Attribute) (err error) {
	obj, err := tn.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	data := obj.Data()
	newValue, err := sjson.SetRaw(string(data), tn.id.String(), attr.GetJSON())
	if err != nil {
		return
	}
	err = obj.Update(ctx, tx, []byte(newValue))
	if err != nil {
		return
	}

	updateTable, ok := tn.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("NumberAttribute metainfo dont have updated_table")
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
func (tn *NumberAttributeClass) Delete(ctx context.Context, tx tx.WriteTx, oid common.ObjectId) (err error) {
	obj, err := tn.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	data := obj.Data()
	newValue, err := sjson.Delete(string(data), tn.id.String())
	if err != nil {
		return
	}
	err = obj.Update(ctx, tx, []byte(newValue))
	if err != nil {
		return
	}
	updateTable, ok := tn.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("NumberAttribute metainfo dont have updated_table")
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

func (tn *NumberAttributeClass) Drop(ctx context.Context, tx tx.WriteTx) (err error) {

	updateTable, ok := tn.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("NumberAttribute metainfo dont have updated_table")
		return
	}
	//先删除相关表的索引
	tidList := []common.TableId{}
	queryTableId := `
	SELECT table_id FROM table_to_attribute_classes WHERE class_id = ?`
	rows, err := tx.Query(queryTableId, tn.id)
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
		table, err = tn.db.OpenTable(ctx, tx, tid)
		if err != nil {
			return
		}
		table.DeleteAttributeClass(ctx, tx, tn)
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
		obj, err = tn.db.OpenObject(ctx, tx, oid)
		if err != nil {
			return
		}
		data := obj.Data()
		newValue, err = sjson.Delete(string(data), tn.id.String())
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
	if _, err = tx.Exac(deleteAttributeClassStmt, tn.id); err != nil {
		return
	}
	return
}

func (tn *NumberAttributeClass) FromObject(obj common.Object) (attr common.Attribute, err error) {
	attrNum := &NumberAttribute{
		class: tn,
		value: 0,
	}
	attr = attrNum

	data := obj.Data()
	attrPath := fmt.Sprintf(`%v`, tn.id)
	attrData := gjson.Get(string(data), attrPath)
	if attrData.Type == gjson.Null {
		return
	}

	if err = attrNum.Parse(attrData.Raw); err != nil {
		return
	}

	return
}

// 构建查询
func (tn *NumberAttributeClass) BuildQuery(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (stmt string, err error) {
	if _, ok := v["op"].(string); !ok {
		err = fmt.Errorf("invaild query value:%s", v)
		return
	}
	if _, ok := v["value"].(string); !ok {
		err = fmt.Errorf("invaild query value:%s", v)
		return
	}
	op := v["op"].(string)
	value := v["value"].(float64)

	jsonPath, ok := tn.metaInfo["json_value_path"].(string)
	if !ok {
		err = fmt.Errorf("TextAttribute metainfo dont have json_value_path")
		return
	}
	switch op {
	case "gt":
		stmt = fmt.Sprintf(
			`(data ->> '%s' > %f)`,
			jsonPath,
			value,
		)
	case "gte":
		stmt = fmt.Sprintf(
			`(data ->> '%s' >= %f)`,
			jsonPath,
			value,
		)
	case "lt":
		stmt = fmt.Sprintf(
			`(data ->> '%s' < %f)`,
			jsonPath,
			value,
		)
	case "lte":
		stmt = fmt.Sprintf(
			`(data ->> '%s' <= %f)`,
			jsonPath,
			value,
		)
	case "eq":
		stmt = fmt.Sprintf(
			`(data ->> '%s' = %f)`,
			jsonPath,
			value,
		)
	case "neq":
		stmt = fmt.Sprintf(
			`(data ->> '%s' != %f)`,
			jsonPath,
			value,
		)
	default:
		err = fmt.Errorf("unsupport op:%s", op)
	}
	return
}

// 构建排序
func (tn *NumberAttributeClass) BuildSort(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (stmt string, err error) {
	if _, ok := v["mode"].(string); !ok {
		err = fmt.Errorf("invaild sort value:%s", v)
		return
	}
	mode := v["mode"].(string)
	jsonPath, ok := tn.metaInfo["json_value_path"].(string)
	if !ok {
		err = fmt.Errorf("NumberAttribute metainfo dont have json_value_path")
		return
	}
	switch mode {
	case "asc":
		stmt = fmt.Sprintf(" data ->> '%s' ASC ", jsonPath)
	case "desc":
		stmt = fmt.Sprintf(" data ->> '%s' DESC ", jsonPath)

	}
	return
}

func (t *NumberAttribute) GetJSON() string {
	return fmt.Sprintf(`{"value":%f}`, t.value)
}
func (t *NumberAttribute) String() string {
	return fmt.Sprintf("%f", t.value)
}
func (t *NumberAttribute) GetClass() common.AttributeClass {
	return t.class
}
func (t *NumberAttribute) SetValue(v map[string]interface{}) (err error) {
	if value, ok := v["value"].(int); ok {
		t.value = float64(value)
		return
	}
	if value, ok := v["value"].(float64); ok {
		t.value = value
		return
	}
	err = fmt.Errorf("invaild set value:%v", v)
	return
}
func (t *NumberAttribute) Parse(v string) error {
	result := gjson.Get(v, "value")
	if result.Type != gjson.Number {
		return fmt.Errorf("parse error: %v", v)
	}
	t.value = result.Num
	return nil
}
