package attribute

import (
	"bytes"
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

const ZWSP = "\u200d"

type LinkAttributeClass struct {
	AttributeClassInfo
}

type LinkAttribute struct {
	class *LinkAttributeClass
	raw   []common.ObjectId
	show  map[common.ObjectId]string
}

func createLinkAttributeClass(db common.Database, tx tx.WriteTx) (act *LinkAttributeClass, err error) {
	id, err := common.NewAttributeClassId()
	if err != nil {
		return
	}
	jsonValuePath := fmt.Sprintf(`$."%v"."value"`, id)
	updateTable := fmt.Sprintf(`link_%v`, id)
	linkRefTable := fmt.Sprintf(`link_ref_%v`, id)
	ref_table := ""

	act = &LinkAttributeClass{
		AttributeClassInfo{
			db:       db,
			id:       id,
			name:     "link",
			key:      id.String(),
			attrType: AttributeTypeLink,
			metaInfo: utils.JSONMap{
				"json_value_path":    jsonValuePath,
				"gjson_value_path":   "value",
				"gjson_idx_path":     "idx",
				"updated_table":      updateTable,
				"link_obj_table":     linkRefTable,
				"ref_table":          ref_table,
				"ref_link_attribute": "",
				"dep_attribute":      "[]",
			},
		},
	}
	createUpdate := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %v(
		object_id BLOB PRIMARY KEY,
		updated BLOB NOT NULL,
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
	)`, updateTable)

	createLinkData := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %v(
		object_id BLOB NOT NULL,
		ref_object_id BLOB NOT NULL
  );
	CREATE INDEX IF NOT EXISTS %v_object_id_idx ON %v (object_id);
	CREATE INDEX IF NOT EXISTS %v_ref_object_id_idx ON %v (ref_object_id);
	`,
		linkRefTable,
		linkRefTable, linkRefTable,
		linkRefTable, linkRefTable,
	)

	if _, err = tx.Exac(createUpdate); err != nil {
		return
	}

	if _, err = tx.Exac(createLinkData); err != nil {
		return
	}
	return
}

func newLinkAttributeClass(ctx context.Context, db common.Database, tx tx.WriteTx) (ac common.AttributeClass, err error) {

	act1, err := createLinkAttributeClass(db, tx)
	if err != nil {
		return
	}
	act2, err := createLinkAttributeClass(db, tx)
	if err != nil {
		return
	}
	act1.metaInfo["ref_link_attribute"] = act2.id.String()
	act2.metaInfo["ref_link_attribute"] = act1.id.String()
	ac = act1
	stmt := `
  INSERT INTO attribute_classes
  (class_id,attribute_name,attribute_key,attribute_type,attribute_meta_info)
  VALUES
  (?,?,?,?,?)`

	if _, err = tx.Exac(stmt, act1.id, act1.name, act1.key, act1.attrType, act1.metaInfo); err != nil {
		return
	}
	if _, err = tx.Exac(stmt, act2.id, act2.name, act2.key, act2.attrType, act2.metaInfo); err != nil {
		return
	}
	act1.registerHookFunc(ctx, tx)
	act2.registerHookFunc(ctx, tx)

	return
}

func parseLinkAttributeClass(ctx context.Context, tx tx.ReadTx, acProto *AttributeClassInfo) (ac common.AttributeClass, err error) {
	acl := &LinkAttributeClass{*acProto}
	acl.registerHookFunc(ctx, tx)
	ac = acl
	return

}

func (lc *LinkAttributeClass) registerHookFunc(_ context.Context, _ tx.ReadTx) (nerr error) {

	//获取关联的link
	var reflinkAcid common.AttributeClassId
	ref_link, ok := lc.metaInfo["ref_link_attribute"].(string)
	if !ok {
		nerr = fmt.Errorf("LinkAttributeClass %v ref link error", lc.id)
		return
	}
	nerr = reflinkAcid.Scan(ref_link)
	if nerr != nil {
		return
	}

	// 获取关联的dep_attribute
	depAttributeListStr, ok := lc.metaInfo["dep_attribute"].(string)
	if !ok {
		nerr = fmt.Errorf("LinkAttributeClass %v dep attribute error", lc.id)
		return
	}
	depIdList := []common.AttributeClassId{}
	if !gjson.Valid(depAttributeListStr) {
		nerr = fmt.Errorf("LinkAttributeClass:error json:%v", depAttributeListStr)
		return
	}
	gjson.Parse(depAttributeListStr).ForEach(func(key, value gjson.Result) bool {
		if value.Type != gjson.String {
			nerr = fmt.Errorf("LinkAttributeClass %v dep attribute id invaild:%v", lc.id, value.Value())
			return false
		}
		idStr := value.Str
		var acid common.AttributeClassId
		nerr = acid.Scan(idStr)
		if nerr != nil {
			return false
		}
		depIdList = append(depIdList, acid)
		return true
	})
	if nerr != nil {
		return
	}

	afterF := func(ctx context.Context, db common.Database, tx tx.WriteTx, op common.AttributeOp) (err error) {

		// 假如当前更新操作的classid是本linkClass，就更新本linkClass对应的ref_link的show和raw
		if op.ClassId().String() == lc.id.String() {
			err = refreshLink(ctx, db, tx, op, reflinkAcid)
			return
		}
		// 假如当前更新操作的classid是本linkClass依赖的属性，就更新本link的show
		for _, acid := range depIdList {
			if op.ClassId().String() == acid.String() {
				err = refreshRefLink(ctx, db, tx, op, lc)
				return
			}
		}
		return
	}
	nerr = common.RegisterAfterAttributeHook(lc.id, afterF)
	return
}

func refreshLink(ctx context.Context, db common.Database, tx tx.WriteTx, op common.AttributeOp, refAcid common.AttributeClassId) (err error) {
	ref, err := db.OpenAttributeClass(ctx, tx, refAcid)
	if err != nil {
		return
	}
	refLink, ok := ref.(*LinkAttributeClass)
	if !ok {
		err = fmt.Errorf("link ref attributeclass is not a link")
		return
	}
	linkAttr, ok := op.Attribute().(*LinkAttribute)
	if !ok {
		err = fmt.Errorf("link hook func reciver unlink attribute")
		return
	}
	oidList := linkAttr.raw
	switch op.Op() {
	case common.InsertAttribute:
		return
	case common.UpdateAttribute:
		for _, oid := range oidList {
			var refLinkAttr common.Attribute

			refLinkAttr, nerr := refLink.FindId(ctx, tx, oid)
			if nerr != nil {
				if nerr != sql.ErrNoRows {
					err = nerr
					return
				}
				refLinkAttr, err = refLink.Insert(ctx, tx, oid)
				if err != nil {
					return
				}
				refLinkAttrImpl, ok := refLinkAttr.(*LinkAttribute)
				if !ok {
					err = fmt.Errorf("reflink insert attr and get unlink attribute")
					return
				}
				refLinkAttrImpl.AddObject(ctx, tx, op.Object().ObjectId())
				err = refLink.Update(ctx, tx, oid, refLinkAttrImpl)

				if err != nil {
					return
				}
			}
			refLinkAttrImpl, ok := refLinkAttr.(*LinkAttribute)
			if !ok {
				err = fmt.Errorf("reflink findId attr and get unlink attribute")
				return
			}
			// 检测是否已存在oid
			_, ok = refLinkAttrImpl.show[op.Object().ObjectId()]
			if ok {
				return
			}
			err = refLinkAttrImpl.AddObject(ctx, tx, op.Object().ObjectId())
			if err != nil {
				return
			}
			err = refLink.Update(ctx, tx, oid, refLinkAttrImpl)
			if err != nil {
				return
			}
		}
	case common.DeleteAttribute:
		for _, oid := range oidList {
			refLinkAttr, nerr := refLink.FindId(ctx, tx, oid)
			if nerr != nil {
				if nerr == sql.ErrNoRows {
					continue
				} else {
					err = nerr
					return
				}
			}
			refLinkAttrImpl, ok := refLinkAttr.(*LinkAttribute)
			if !ok {
				err = fmt.Errorf("reflink findId attr and get unlink attribute")
				return
			}
			err = refLinkAttrImpl.DeleteOneObject(ctx, tx, oid)
			if err != nil {
				return
			}
			err = refLink.Update(ctx, tx, oid, refLinkAttrImpl)
			if err != nil {
				return
			}
		}
	}
	return
}

// 假如当前更新操作的obj存在origin link的ref link，就更新ref link oidlist 对应的origin link的show
func refreshRefLink(ctx context.Context, db common.Database, tx tx.WriteTx, op common.AttributeOp, origin *LinkAttributeClass) (err error) {

	//获取关联的link
	var refLinkAcid common.AttributeClassId
	ref_link, ok := origin.metaInfo["ref_link_attribute"].(string)
	if !ok {
		err = fmt.Errorf("LinkAttributeClass %v ref link error", origin.id)
		return
	}

	err = refLinkAcid.Scan(ref_link)
	if err != nil {
		return
	}
	// 查找是否有reflink的属性
	findRef := false
	if op.Object() == nil || op.Object().Data() == nil {
		return
	}
	data := op.Object().Data()
	gjson.ParseBytes(data).ForEach(func(key, value gjson.Result) bool {
		if key.Str == ref_link {
			findRef = true
			return false
		}
		return true
	})
	if !findRef {
		return
	}
	refLinkAc, err := db.OpenAttributeClass(ctx, tx, refLinkAcid)
	if err != nil {
		return
	}
	Attr, err := refLinkAc.FromObject(op.Object())
	if err != nil {
		return
	}
	refLinkAttr, ok := Attr.(*LinkAttribute)
	if !ok {
		err = fmt.Errorf("ref link hook func reciver unlink attribute")
		return
	}
	oidList := refLinkAttr.raw
	switch op.Op() {
	case common.InsertAttribute:
		return
	case common.UpdateAttribute:
		for _, oid := range oidList {
			var originAttr common.Attribute
			originAttr, err = origin.FindId(ctx, tx, oid)
			if err != nil {
				return
			}
			originLinkAttr, ok := originAttr.(*LinkAttribute)
			if !ok {
				err = fmt.Errorf("origin link hook func reciver unlink attribute")
				return
			}
			err = originLinkAttr.updateObjectShow(ctx, tx, op.Object().ObjectId())
			if err != nil {
				return
			}
		}
	case common.DeleteAttribute:
		for _, oid := range oidList {
			var originAttr common.Attribute
			originAttr, err = origin.FindId(ctx, tx, oid)
			if err != nil {
				return
			}
			originLinkAttr, ok := originAttr.(*LinkAttribute)
			if !ok {
				err = fmt.Errorf("origin link hook func reciver unlink attribute")
				return
			}
			err = originLinkAttr.DeleteOneObject(ctx, tx, op.Object().ObjectId())
			if err != nil {
				return
			}
		}
	}
	return
}

func (lc *LinkAttributeClass) GetMetaInfo(ctx context.Context, tx tx.ReadTx) (v utils.JSONMap, err error) {
	m := utils.JSONMap{}
	for key := range lc.metaInfo {
		m[key] = lc.metaInfo[key]
	}
	return m, nil
}

// "ref_table":          tableId,
// "ref_link_attribute": acid,
// "dep_attribute":      "[]",
func (lc *LinkAttributeClass) Set(ctx context.Context, tx tx.WriteTx, v utils.JSONMap) (err error) {
	oldName := lc.name
	oldkey := lc.key
	oldMetaInfo := utils.JSONMap{}
	for key := range lc.metaInfo {
		oldMetaInfo[key] = lc.metaInfo[key]
	}
	defer func() {
		if err != nil {
			lc.name = oldName
			lc.key = oldkey
			lc.metaInfo = oldMetaInfo
			lc.registerHookFunc(ctx, tx)
		}
	}()

	if name, ok := v["name"]; ok {
		switch value := name.(type) {
		case string:
			lc.name = value
		default:
			err = fmt.Errorf("set name with error type")
			return
		}
		// delete(v, "name")
	}

	if key, ok := v["key"]; ok {
		switch value := key.(type) {
		case string:
			lc.key = value
		default:
			err = fmt.Errorf("set key with error type")
			return
		}
		// delete(v, "key")
	}

	if refTable, ok := v["ref_table"]; ok {
		switch value := refTable.(type) {
		case string:
			lc.metaInfo["ref_table"] = value
		case common.TableId:
			lc.metaInfo["ref_table"] = value.String()
		default:
			err = fmt.Errorf("set ref_table with error type")
			return
		}
		// delete(v, "ref_table")
	}

	if refLink, ok := v["ref_link_attribute"]; ok {
		switch value := refLink.(type) {
		case string:
			lc.metaInfo["ref_link_attribute"] = value
		case common.AttributeClassId:
			lc.metaInfo["ref_link_attribute"] = value.String()
		case common.AttributeClass:
			lc.metaInfo["ref_link_attribute"] = value.ClassId().String()
		default:
			err = fmt.Errorf("set ref_link_attribute with error type")
			return
		}
		// delete(v, "ref_link_attribute")
	}

	if depAttributeList, ok := v["dep_attribute"]; ok {
		switch value := depAttributeList.(type) {
		case string:
			lc.metaInfo["dep_attribute"] = value
		case []common.AttributeClassId:
			buf := &bytes.Buffer{}
			buf.WriteString("[")
			for idx, acid := range value {
				if idx != 0 {
					buf.WriteString(",")
				}
				buf.WriteString(fmt.Sprintf(`"%v"`, acid))
			}
			buf.WriteString("]")
			lc.metaInfo["dep_attribute"] = buf.String()
		case []common.AttributeClass:
			buf := &bytes.Buffer{}
			buf.WriteString("[")
			for idx, ac := range value {
				if idx != 0 {
					buf.WriteString(",")
				}
				buf.WriteString(fmt.Sprintf(`"%v"`, ac.ClassId()))
			}
			buf.WriteString("]")
			lc.metaInfo["dep_attribute"] = buf.String()
		default:
			err = fmt.Errorf("set dep_attribute with error type")
			return
		}
		err = lc.registerHookFunc(ctx, tx)
		if err != nil {
			return
		}
		// delete(v, "dep_attribute")

	}
	stmt := `
	UPDATE attribute_classes
	SET (attribute_name,attribute_key,attribute_meta_info) =
	(?,?,?)
	WHERE class_id = ?`
	if _, err = tx.Exac(stmt, lc.name, lc.key, lc.metaInfo, lc.id); err != nil {
		return
	}
	return
}

func (lc *LinkAttributeClass) Insert(ctx context.Context, tx tx.WriteTx, oid common.ObjectId) (attr common.Attribute, err error) {

	attrLink := &LinkAttribute{
		class: lc,
		raw:   []common.ObjectId{},
		show:  map[common.ObjectId]string{},
	}
	attr = attrLink

	obj, err := lc.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}

	//hook
	lc.DoPreHook(ctx, lc.db, tx, NewOp(lc.id, obj, common.InsertAttribute, attr))
	defer func() { lc.DoAfterHook(ctx, lc.db, tx, NewOp(lc.id, obj, common.InsertAttribute, attr)) }()
	//hook

	data := obj.Data()
	newValue, err := sjson.SetRaw(string(data), lc.id.String(), attr.GetJSON())
	if err != nil {
		return
	}
	err = obj.Update(ctx, tx, []byte(newValue))
	if err != nil {
		return
	}

	updateTable, ok := lc.metaInfo["updated_table"].(string)
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
func (lc *LinkAttributeClass) FindId(ctx context.Context, tx tx.ReadTx, oid common.ObjectId) (attr common.Attribute, err error) {

	obj, err := lc.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	data := obj.Data()
	attrPath := fmt.Sprintf(`%v`, lc.id)
	attrData := gjson.Get(string(data), attrPath)
	if attrData.Type == gjson.Null {
		err = sql.ErrNoRows
		return
	}
	attrLink := &LinkAttribute{
		class: lc,
		raw:   []common.ObjectId{},
		show:  map[common.ObjectId]string{},
	}
	if err = attrLink.Parse(attrData.Raw); err != nil {
		return
	}
	attr = attrLink
	return
}
func (lc *LinkAttributeClass) Update(ctx context.Context, tx tx.WriteTx, oid common.ObjectId, attr common.Attribute) (err error) {

	obj, err := lc.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}

	attrLink, ok := attr.(*LinkAttribute)
	if !ok {
		err = fmt.Errorf("update linkAttribute Class get an unlinkattribute")
	}
	for _, refOid := range attrLink.raw {
		attrLink.updateObjectShow(ctx, tx, refOid)
	}

	//hook
	lc.DoPreHook(ctx, lc.db, tx, NewOp(lc.id, obj, common.UpdateAttribute, attr))
	defer func() { lc.DoAfterHook(ctx, lc.db, tx, NewOp(lc.id, obj, common.UpdateAttribute, attr)) }()
	//hook

	data := obj.Data()
	newValue, err := sjson.SetRaw(string(data), lc.id.String(), attr.GetJSON())
	if err != nil {
		return
	}
	err = obj.Update(ctx, tx, []byte(newValue))
	if err != nil {
		return
	}

	updateTable, ok := lc.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("linkAttributeClass metainfo dont have updated_table")
		return
	}

	linkObjTable, ok := lc.metaInfo["link_obj_table"].(string)
	if !ok {
		err = fmt.Errorf("linkAttributeClass metainfo dont have link_obj_table")
		return
	}
	update := fmt.Sprintf(`
UPDATE %s SET updated = ?
  WHERE object_id = ?
    `, updateTable)
	opId := xid.New()

	deleteLinkTable := fmt.Sprintf(`
	DELETE FROM %s WHERE object_id = ?`, linkObjTable)

	updateLinkTable := fmt.Sprintf(`
	INSERT INTO %s (object_id,ref_object_id) VALUES (?,?)`, linkObjTable)

	if _, err = tx.Exac(update, opId, oid); err != nil {
		return
	}

	if _, err = tx.Exac(deleteLinkTable, oid); err != nil {
		return
	}

	for _, refOid := range attrLink.raw {
		if _, err = tx.Exac(updateLinkTable, oid, refOid); err != nil {
			return
		}
	}

	return
}

func (lc *LinkAttributeClass) Delete(ctx context.Context, tx tx.WriteTx, oid common.ObjectId) (err error) {

	obj, err := lc.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}

	//hook
	lc.DoPreHook(ctx, lc.db, tx, NewOp(lc.id, obj, common.DeleteAttribute, nil))
	defer func() { lc.DoAfterHook(ctx, lc.db, tx, NewOp(lc.id, obj, common.DeleteAttribute, nil)) }()
	//hook

	data := obj.Data()
	newValue, err := sjson.Delete(string(data), lc.id.String())
	if err != nil {
		return
	}
	err = obj.Update(ctx, tx, []byte(newValue))
	if err != nil {
		return
	}
	updateTable, ok := lc.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("NumberAttribute metainfo dont have updated_table")
		return
	}

	linkObjTable, ok := lc.metaInfo["link_obj_table"].(string)
	if !ok {
		err = fmt.Errorf("linkAttributeClass metainfo dont have link_obj_table")
		return
	}
	deleteRecord := fmt.Sprintf(`
DELETE FROM %s WHERE object_id = ?
`, updateTable)

	deleteLinkTable := fmt.Sprintf(`
DELETE FROM %s WHERE object_id = ?`, linkObjTable)

	if _, err = tx.Exac(deleteRecord, oid); err != nil {
		return
	}

	if _, err = tx.Exac(deleteLinkTable, oid); err != nil {
		return
	}

	return
}

func (lc *LinkAttributeClass) Drop(ctx context.Context, tx tx.WriteTx) (err error) {

	var refLinkAcid common.AttributeClassId
	ref_link, ok := lc.metaInfo["ref_link_attribute"].(string)
	if !ok {
		err = fmt.Errorf("LinkAttributeClass %v ref link error", lc.id)
		return
	}

	err = refLinkAcid.Scan(ref_link)
	if err != nil {
		return
	}
	refAc, err := lc.db.OpenAttributeClass(ctx, tx, refLinkAcid)
	if err != nil {
		return
	}
	refLinkAc, ok := refAc.(*LinkAttributeClass)
	if !ok {
		err = fmt.Errorf("link ref attributeclass is not a link")
		return
	}
	err = dropLinkAttributeClass(ctx, tx, lc)
	if err != nil {
		return
	}
	err = dropLinkAttributeClass(ctx, tx, refLinkAc)
	if err != nil {
		return
	}
	return
}

func dropLinkAttributeClass(ctx context.Context, tx tx.WriteTx, lc *LinkAttributeClass) (err error) {
	updateTable, ok := lc.metaInfo["updated_table"].(string)
	if !ok {
		err = fmt.Errorf("LinkAttribute metainfo dont have updated_table")
		return
	}
	linkObjTable, ok := lc.metaInfo["link_obj_table"].(string)
	if !ok {
		err = fmt.Errorf("linkAttributeClass metainfo dont have link_obj_table")
		return
	}
	//先删除相关表的索引
	tidList := []common.TableId{}
	queryTableId := `
	SELECT table_id FROM table_to_attribute_classes WHERE class_id = ?`
	rows, err := tx.Query(queryTableId, lc.id)
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
		table, err = lc.db.OpenTable(ctx, tx, tid)
		if err != nil {
			return
		}
		table.DeleteAttributeClass(ctx, tx, lc)
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
		obj, err = lc.db.OpenObject(ctx, tx, oid)
		if err != nil {
			return
		}
		data := obj.Data()
		newValue, err = sjson.Delete(string(data), lc.id.String())
		if err != nil {
			return
		}
		err = obj.Update(ctx, tx, []byte(newValue))
		if err != nil {
			return
		}
	}

	dropTable := fmt.Sprintf(`
DROP TABLE %s;
DROP TABLE %s;`,
		updateTable, linkObjTable)
	if _, err = tx.Exac(dropTable); err != nil {
		return
	}

	deleteAttributeClassStmt := `DELETE FROM attribute_classes WHERE class_id = ?`
	if _, err = tx.Exac(deleteAttributeClassStmt, lc.id); err != nil {
		return
	}
	return
}

func (lc *LinkAttributeClass) FromObject(obj common.Object) (attr common.Attribute, err error) {

	attrLink := &LinkAttribute{
		class: lc,
		raw:   []common.ObjectId{},
		show:  map[common.ObjectId]string{},
	}
	attr = attrLink

	data := obj.Data()
	attrPath := fmt.Sprintf(`%v`, lc.id)
	attrData := gjson.Get(string(data), attrPath)
	if attrData.Type == gjson.Null {
		return
	}

	if err = attrLink.Parse(attrData.Raw); err != nil {
		return
	}
	return
}

// 构建查询
func (nc *LinkAttributeClass) BuildQuery(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (stmt string, err error) {
	//TODO
	panic("no impl")
}

// 构建排序
func (nc *LinkAttributeClass) BuildSort(ctx context.Context, tx tx.ReadTx, v map[string]interface{}) (stmt string, err error) {
	//TODO
	panic("no impl")
}

// 返回形如：
// {value:{oid1:str1,oid2:str2},idx:"str1|str2"}
func (t *LinkAttribute) GetJSON() string {
	buf := &bytes.Buffer{}
	strBuf := &bytes.Buffer{}
	buf.WriteString("{")
	for idx, oid := range t.raw {
		if idx != 0 {
			buf.WriteString(",")
			strBuf.WriteString(ZWSP)
		}
		buf.WriteString(fmt.Sprintf(`"%v":"%v"`, oid, t.show[oid]))
		strBuf.WriteString(fmt.Sprintf("%v", t.show[oid]))
	}
	buf.WriteString("}")
	ret := fmt.Sprintf(`{value:%s,idx:"%s"}`, buf.String(), strBuf.String())

	return ret

}
func (t *LinkAttribute) String() string {
	strBuf := &bytes.Buffer{}
	for idx, oid := range t.raw {
		if idx != 0 {
			strBuf.WriteString(ZWSP)
		}

		strBuf.WriteString(fmt.Sprintf("%v", t.show[oid]))
	}
	ret := strBuf.String()
	return ret
}
func (t *LinkAttribute) GetClass() common.AttributeClass {
	return t.class
}

// 接受 add, delete, update三种传入
// add 传入string,[]oid
// delete 传入string,[]oid
// update三种传入 传入string,[]oid
func (t *LinkAttribute) SetValue(v map[string]interface{}) (err error) {
	ctx, ok := v["ctx"].(context.Context)
	if !ok {
		err = fmt.Errorf("link attr get ctx failed")
		return
	}
	tx, ok := v["tx"].(tx.ReadTx)
	if !ok {
		err = fmt.Errorf("link attr get ctx failed")
		return
	}
	if update, ok := v["update"]; ok {
		switch updateValue := update.(type) {
		case string:
			oidList := []common.ObjectId{}
			gjson.Parse(updateValue).ForEach(func(key, value gjson.Result) bool {
				var oid common.ObjectId
				if value.Type != gjson.String {
					err = fmt.Errorf("parse idlist json failed")
					return false
				}
				err = oid.Scan(value.Str)
				if err != nil {
					return false
				}
				oidList = append(oidList, oid)
				return true
			})
			if err != nil {
				return
			}
			t.raw = []common.ObjectId{}
			t.show = map[common.ObjectId]string{}
			t.AddObject(ctx, tx, oidList...)
			return
		case []common.ObjectId:
			t.raw = []common.ObjectId{}
			t.show = map[common.ObjectId]string{}
			t.AddObject(ctx, tx, updateValue...)
			return
		default:
			err = fmt.Errorf("unsupport update link data type")
			return
		}
	}
	if delete, ok := v["delete"]; ok {
		switch deleteValue := delete.(type) {
		case string:
			oidList := []common.ObjectId{}
			gjson.Parse(deleteValue).ForEach(func(key, value gjson.Result) bool {
				var oid common.ObjectId
				if value.Type != gjson.String {
					err = fmt.Errorf("parse idlist json failed")
					return false
				}
				err = oid.Scan(value.Str)
				if err != nil {
					return false
				}
				oidList = append(oidList, oid)
				return true
			})
			if err != nil {
				return
			}
			t.DeleteObject(ctx, tx, oidList...)
			return
		case []common.ObjectId:
			t.DeleteObject(ctx, tx, deleteValue...)
			return
		default:
			err = fmt.Errorf("unsupport delete link data type")
			return
		}
	}

	if add, ok := v["add"]; ok {
		switch addValue := add.(type) {
		case string:
			oidList := []common.ObjectId{}
			gjson.Parse(addValue).ForEach(func(key, value gjson.Result) bool {
				var oid common.ObjectId
				if value.Type != gjson.String {
					err = fmt.Errorf("parse idlist json failed")
					return false
				}
				err = oid.Scan(value.Str)
				if err != nil {
					return false
				}
				oidList = append(oidList, oid)
				return true
			})
			if err != nil {
				return
			}
			t.AddObject(ctx, tx, oidList...)
			return
		case []common.ObjectId:
			t.AddObject(ctx, tx, addValue...)
			return
		default:
			err = fmt.Errorf("unsupport delete link data type")
			return
		}
	}
	err = fmt.Errorf("unsupport link attr set op")
	return
}
func (t *LinkAttribute) Parse(v string) (err error) {
	result := gjson.Parse(v).Get("value")
	if result.Type != gjson.JSON {
		err = fmt.Errorf("parse link value error")
		return
	}
	oidList := []common.ObjectId{}
	showMap := map[common.ObjectId]string{}
	result.ForEach(func(key, value gjson.Result) bool {
		var oid common.ObjectId
		if key.Type != gjson.String {
			err = fmt.Errorf("parse link key json failed")
			return false
		}
		if value.Type != gjson.String {
			err = fmt.Errorf("parse link value json failed")
			return false
		}
		err = oid.Scan(key.Str)
		if err != nil {
			return false
		}
		oidList = append(oidList, oid)
		showMap[oid] = value.Str
		return true
	})
	if err != nil {
		return
	}
	t.raw = oidList
	t.show = showMap
	return

}

func (t *LinkAttribute) AddObject(ctx context.Context, tx tx.ReadTx, oidList ...common.ObjectId) (err error) {

	// 先检测oid是否已经关联，否则获取字符表达
	for _, oid := range oidList {
		if _, ok := t.show[oid]; ok {
			continue
		}
		t.updateObjectShow(ctx, tx, oid)
		t.raw = append(t.raw, oid)
	}
	return
}

func (t *LinkAttribute) updateObjectShow(ctx context.Context, tx tx.ReadTx, oid common.ObjectId) (err error) {
	// 获取依赖的attribute
	depAcListInfo, ok := t.class.metaInfo["dep_attribute"]
	if !ok {
		err = fmt.Errorf("link ac unfound dep attribute")
	}
	depAcListstr, ok := depAcListInfo.(string)
	if !ok {
		err = fmt.Errorf("invaild dep attribute in metainfo")
	}
	depAcList := []common.AttributeClass{}
	gjson.Parse(depAcListstr).ForEach(func(key, value gjson.Result) bool {
		if value.Type != gjson.String {
			err = fmt.Errorf("invaild dep attribute in metainfo")
			return false
		}
		idStr := value.Str
		var acid common.AttributeClassId
		err = acid.Scan(idStr)
		if err != nil {
			return false
		}
		var ac common.AttributeClass
		ac, err = t.class.db.OpenAttributeClass(ctx, tx, acid)
		depAcList = append(depAcList, ac)
		return true
	})
	if err != nil {
		return
	}
	//读取oid对应的object并更新show
	var obj common.Object
	obj, err = t.class.db.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	showStrBuffer := &bytes.Buffer{}
	for idx, ac := range depAcList {
		if idx != 0 {
			showStrBuffer.WriteString(ZWSP)
		}
		var attr common.Attribute
		attr, err = ac.FromObject(obj)
		if err != nil {
			return
		}
		showStrBuffer.WriteString(attr.String())
	}
	t.show[oid] = showStrBuffer.String()
	return
}

func (t *LinkAttribute) DeleteObject(ctx context.Context, tx tx.ReadTx, oidList ...common.ObjectId) (err error) {
	for _, doid := range oidList {
		t.DeleteOneObject(ctx, tx, doid)
	}
	return
}

func (t *LinkAttribute) DeleteOneObject(ctx context.Context, tx tx.ReadTx, doid common.ObjectId) (err error) {
	newOidList := []common.ObjectId{}
	for _, oid := range t.raw {
		if oid.String() == doid.String() {
			delete(t.show, oid)
			continue
		}
		newOidList = append(newOidList, oid)
	}
	t.raw = newOidList
	return

}
