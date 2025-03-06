package paroket

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"paroket/common"
	"paroket/tx"
	"paroket/utils"
	"sync"

	"github.com/tidwall/gjson"
)

const createFtsTriggerTemplate = `CREATE TRIGGER %s AFTER UPDATE OF data ON %s 
BEGIN
UPDATE %s SET idx =%s
WHERE object_id = NEW.object_id;
END;`

type tableImpl struct {
	lock      *sync.Mutex
	db        common.Database
	tableId   common.TableId
	tableName string
	fields    []common.AttributeClassId
	metaInfo  utils.JSONMap
	version   int64
}

func init() {
	err := common.RegisterAfterUpdateObjectHook(afterUpdateObject)
	if err != nil {
		fmt.Printf("init table upadate hook:%v", err)
	}
}

func newTable(ctx context.Context, db common.Database, tx tx.WriteTx) (table common.Table, err error) {
	id, err := common.NewTableId()
	if err != nil {
		return
	}
	dataTable := fmt.Sprintf("table_%s", id.String())
	ftsTrigger := fmt.Sprintf("table_%s_fts_trigger", id.String())
	t := &tableImpl{
		lock:      &sync.Mutex{},
		db:        db,
		tableId:   id,
		tableName: "untitled",
		fields:    []common.AttributeClassId{},
		metaInfo: utils.JSONMap{
			"data_table":  dataTable,
			"fts_trigger": ftsTrigger,
		},
		version: 0,
	}
	table = t

	createTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		object_id BLOB PRIMARY KEY,
    	data JSONB,
		idx BLOB DEFAULT '',
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
	);
	`, dataTable)
	_, err = tx.Exac(createTable)
	if err != nil {
		return
	}

	insertTable := `
  INSERT INTO tables 
  (table_id,table_name,meta_info,version) 
  VALUES
  (?,?,?,?)`
	_, err = tx.Exac(insertTable, t.tableId, t.tableName, t.metaInfo, t.version)
	if err != nil {
		return
	}
	// createFtsTrigger := fmt.Sprintf(
	// 	createFtsTriggerTemplate,
	// 	ftsTrigger, dataTable, dataTable, `''`,
	// )
	// _, err = tx.Exac(createFtsTrigger)
	// if err != nil {
	// 	return
	// }
	return
}

func queryTable(ctx context.Context, db common.Database, tx tx.ReadTx, tid common.TableId) (table common.Table, err error) {

	t := &tableImpl{
		lock: &sync.Mutex{},
		db:   db,
	}
	table = t
	query := `
	SELECT table_id,table_name,meta_info,version
	FROM tables
	WHERE table_id = ? `
	if err = tx.QueryRow(query, tid).Scan(&t.tableId, &t.tableName, &t.metaInfo, &t.version); err != nil {
		return
	}
	fields := []common.AttributeClassId{}
	queryFields := `
	SELECT class_id 
	FROM table_to_attribute_classes 
	WHERE table_id = ?`
	rows, err := tx.Query(queryFields, tid)
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
	t.fields = fields

	return
}

func afterUpdateObject(ctx context.Context, db common.Database, tx tx.WriteTx, obj common.Object) (err error) {
	tidList := []common.TableId{}
	queryTableId := `SELECT table_id FROM object_to_tables WHERE object_id = ?`
	rows, err := tx.Query(queryTableId, obj.ObjectId())
	if err != nil && err != sql.ErrNoRows {
		return
	}
	for rows.Next() {
		var tableId common.TableId
		if err = rows.Scan(&tableId); err != nil {
			return
		}
		tidList = append(tidList, tableId)
	}
	for _, tid := range tidList {
		var table common.Table
		idxBuffer := &bytes.Buffer{}
		table, err = db.OpenTable(ctx, tx, tid)
		if err != nil {
			return
		}
		fields := table.Fields()
		gjson.ParseBytes(obj.Data()).ForEach(func(key, value gjson.Result) bool {
			for _, acid := range fields {
				if acid.String() == key.Str {
					// TODO
					// 需要加入分隔符
					var ac common.AttributeClass
					var acMetaInfo utils.JSONMap
					ac, err = db.OpenAttributeClass(ctx, tx, acid)
					acMetaInfo, err = ac.GetMetaInfo(ctx, tx)
					idxPath, ok := acMetaInfo["gjson_idx_path"].(string)
					if !ok {
						err = fmt.Errorf("ac metainfo not found gjson_idx_path")
					}
					idxBuffer.WriteString(fmt.Sprintf("%v", value.Get(idxPath).Value()))
				}
			}
			return true
		})
		idxStr := idxBuffer.String()
		updateRelateTable := fmt.Sprintf(`UPDATE %s SET (data,idx) =( jsonb(?), ?) WHERE object_id = ?`, tid.DataTable())
		if _, err = tx.Exac(updateRelateTable, obj.Data(), idxStr, obj.ObjectId()); err != nil {
			return
		}
	}
	return
}

func (t *tableImpl) TableId() common.TableId {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.tableId
}

func (t *tableImpl) Name() string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.tableName
}

func (t *tableImpl) MetaInfo() utils.JSONMap {
	t.lock.Lock()
	defer t.lock.Unlock()
	m := utils.JSONMap{}
	for key := range t.metaInfo {
		m[key] = t.metaInfo[key]
	}
	return m
}
func (t *tableImpl) Fields() []common.AttributeClassId {
	return t.fields
}

func (t *tableImpl) Set(ctx context.Context, tx tx.WriteTx, v utils.JSONMap) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	oldName := t.tableName
	oldMetaInfo := utils.JSONMap{}
	for key := range t.metaInfo {
		oldMetaInfo[key] = t.metaInfo[key]
	}
	defer func() {
		if err != nil {
			t.tableName = oldName
			t.metaInfo = oldMetaInfo
		}
	}()

	if name, ok := v["name"]; ok {
		switch value := name.(type) {
		case string:
			t.tableName = value
		default:
			err = fmt.Errorf("set name with error type")
			return
		}
		delete(v, "name")
	}

	for key := range v {
		t.metaInfo[key] = v[key]
	}
	stmt := `
  UPDATE tables SET
  (table_name,meta_info,version) 
  =
  (?,?,?)
  WHERE table_id = ?`
	if _, err = tx.Exac(stmt, t.tableName, t.metaInfo, t.version, t.tableId); err != nil {
		return
	}
	return
}

func oidListMarshal(oidList []common.ObjectId) string {
	buffer := &bytes.Buffer{}
	buffer.WriteString("[")
	for idx, oid := range oidList {
		if idx != 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString(fmt.Sprintf(`"%v"`, oid))
	}
	buffer.WriteString("]")
	return buffer.String()
}

func (t *tableImpl) FindId(ctx context.Context, tx tx.ReadTx, oidList ...common.ObjectId) (objList []common.Object, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found tablekey")
	}
	stmt := fmt.Sprintf(`
	SELECT object_id,data FROM %s 
	WHERE object_id IN (
	SELECT value FROM json_each(json('%s'))
	)`, dataTable, oidListMarshal(oidList))
	rows, err := tx.Query(stmt)
	if err != nil {
		return
	}
	objList, err = common.QueryTableObjectList(ctx, t.db, rows)
	if err != nil {
		return
	}
	return
}

func (t *tableImpl) Insert(ctx context.Context, tx tx.WriteTx, oidList ...common.ObjectId) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found tablekey")
	}
	stmt := fmt.Sprintf(`
	INSERT INTO %s
    	(object_id,data)
  	VALUES
    	(?,?);`, dataTable)
	InsertTableObjRelation := `
	INSERT INTO object_to_tables
		(object_id, table_id)
	VALUES
		(?,?)`
	for _, oid := range oidList {
		var obj common.Object
		obj, err = t.db.OpenObject(ctx, tx, oid)
		if _, err = tx.Exac(stmt, oid, obj.Data()); err != nil {
			return
		}
		if _, err = tx.Exac(InsertTableObjRelation, oid, t.tableId); err != nil {
			return
		}
		afterUpdateObject(ctx, t.db, tx, obj)
	}
	return
}

func (t *tableImpl) Delete(ctx context.Context, tx tx.WriteTx, oidList ...common.ObjectId) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found dataTable")
	}
	stmt := fmt.Sprintf(`
	DELETE FROM %s WHERE object_id IN (
	SELECT value FROM json_each(json('%s'))
	)`, dataTable, oidListMarshal(oidList))

	for _, oid := range oidList {
		if _, err = tx.Exac(stmt, oid); err != nil {
			return
		}
	}
	return
}

func (t *tableImpl) AddAttributeClass(ctx context.Context, tx tx.WriteTx, ac common.AttributeClass) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// 先确认索引是否在fields中
	if isInFields(t.fields, ac.ClassId()) {
		return
	}

	// 保存fields后新增field
	oldFields := append([]common.AttributeClassId{}, t.fields...)
	defer func() {
		if err != nil {
			t.fields = oldFields
		}
	}()
	t.fields = append(t.fields, ac.ClassId())

	// 读取fields对应的attribute class 方便后面的操作。
	// 这个必须早于获取tx，防止获取tx死锁
	acList := []common.AttributeClass{}
	for _, acid := range t.fields {
		var oldAc common.AttributeClass
		oldAc, err = t.db.OpenAttributeClass(ctx, tx, acid)
		if err != nil {
			return
		}
		acList = append(acList, oldAc)
	}

	// 插入属性-表关联表
	insertFields := `
	INSERT INTO table_to_attribute_classes
	(table_id, class_id) 
	VALUES
	(?, ?)`
	if _, err = tx.Exac(insertFields, t.tableId, ac.ClassId()); err != nil {
		return
	}
	// 创建属性的索引
	metaInfo, err := ac.GetMetaInfo(ctx, tx)
	if err != nil {
		return
	}
	jsonValuePath, ok := metaInfo["json_value_path"]
	if !ok {
		err = fmt.Errorf("add attributeclass to table failed:can not get json path")
		return
	}
	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found dataTable")
	}
	createIndex := fmt.Sprintf(
		`CREATE INDEX idx_%v_%v ON %s(data->>'%s' DESC);`,
		t.tableId, ac.ClassId(), dataTable, jsonValuePath,
	)
	if _, err = tx.Exac(createIndex); err != nil {
		err = fmt.Errorf("%w on stmt:%s", err, createIndex)
		return
	}

	// 修改索引触发器并更新全部索引

	if err = updateFtsIndex(ctx, t, acList, tx); err != nil {
		return
	}

	return
}

// 修改索引触发器并更新全部索引
func updateFtsIndex(ctx context.Context, t *tableImpl, acList []common.AttributeClass, tx tx.WriteTx) (err error) {
	// 目前测试结果使用触发器更新索引比先读取再写入快一倍
	// 5w对象 * 2 表 * 8 属性 的更新：
	// sqlite数据库 4s
	// prepare 6s
	// 无prepare 8.8s

	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found dataTable")
		return
	}

	queryObj := fmt.Sprintf(`
	SELECT object_id, json(data) FROM %s`, dataTable)
	rows, err := tx.Query(queryObj)
	if err != nil {
		return
	}

	// objList, err := common.QueryTableObject(ctx, t.db, rows)
	for rows.Next() {
		var oid common.ObjectId
		var obj common.Object
		rows.Scan(&oid)
		obj, err = common.QueryTableObject(ctx, t.db, rows)
		if err != nil {
			return
		}
		idxBuffer := &bytes.Buffer{}

		gjson.ParseBytes(obj.Data()).ForEach(func(key, value gjson.Result) bool {
			for _, ac := range acList {
				if ac.ClassId().String() == key.Str {
					// TODO
					// 需要加入分隔符
					var acMetaInfo utils.JSONMap
					acMetaInfo, err = ac.GetMetaInfo(ctx, tx)
					idxPath, ok := acMetaInfo["gjson_idx_path"].(string)
					if !ok {
						err = fmt.Errorf("ac metainfo not found gjson_idx_path")
					}
					idxBuffer.WriteString(fmt.Sprintf("%v", value.Get(idxPath).Value()))
				}
			}
			return true
		})
		idxStr := idxBuffer.String()
		updateFtsIdxStmt := fmt.Sprintf(`
	UPDATE %s SET idx = ? WHERE object_id = ?`, dataTable)

		if _, err = tx.Exac(updateFtsIdxStmt, idxStr, obj.ObjectId()); err != nil {
			return
		}
	}

	return
}

func isInFields(acidList []common.AttributeClassId, acid common.AttributeClassId) bool {
	for _, fieldId := range acidList {
		if fieldId == acid {
			return true
		}
	}
	return false
}

func (t *tableImpl) DeleteAttributeClass(ctx context.Context, tx tx.WriteTx, ac common.AttributeClass) (err error) {

	// 先确认索引是否在fields中
	if !isInFields(t.fields, ac.ClassId()) {
		return
	}
	// 出错就恢复fields
	oldFields := append([]common.AttributeClassId{}, t.fields...)
	defer func() {
		if err != nil {
			t.fields = oldFields
		}
	}()
	// 删除t.fields中的对应field
	newFields := []common.AttributeClassId{}
	for _, field := range t.fields {
		if field != ac.ClassId() {
			newFields = append(newFields, field)
		}
	}
	t.fields = newFields

	// 读取fields对应的attribute class 方便后面的操作。
	// 这个必须早于获取tx，防止获取tx死锁
	acList := []common.AttributeClass{}
	for _, acid := range t.fields {
		var oldAc common.AttributeClass
		oldAc, err = t.db.OpenAttributeClass(ctx, tx, acid)
		if err != nil {
			return
		}
		acList = append(acList, oldAc)
	}

	// 删除属性关联表
	insertFields := `
	DELETE FROM table_to_attribute_classes
	WHERE table_id = ? AND class_id = ?;`
	if _, err = tx.Exac(insertFields, t.tableId, ac.ClassId()); err != nil {
		return
	}

	// 删除属性类索引
	dropIndex := fmt.Sprintf(
		`DROP INDEX idx_%v_%v`, t.tableId, ac.ClassId(),
	)
	if _, err = tx.Exac(dropIndex); err != nil {
		return
	}

	// 修改索引触发器并更新全部索引

	if err = updateFtsIndex(ctx, t, acList, tx); err != nil {
		return
	}
	return
}

func (t *tableImpl) NewView(ctx context.Context, tx tx.WriteTx) (view common.View, err error) {
	view, err = newView(ctx, tx, t.db, t)

	return
}

func (t *tableImpl) ListView(ctx context.Context, tx tx.ReadTx) (vlist []common.View, err error) {
	queryVid := `
	SELECT view_id FROM
	table_views
	WHERE table_id = ?`
	rows, err := tx.Query(queryVid, t.tableId)
	if err != nil {
		return
	}
	vidList := []common.ViewId{}
	for rows.Next() {
		var vid common.ViewId
		if err = rows.Scan(&vid); err != nil {
			return
		}
		vidList = append(vidList, vid)
	}
	vlist = []common.View{}
	for _, vid := range vidList {
		var view common.View
		view, err = t.View(ctx, tx, vid)
		if err != nil {
			return
		}
		vlist = append(vlist, view)
	}
	return
}

func (t *tableImpl) View(ctx context.Context, tx tx.ReadTx, vid common.ViewId) (common.View, error) {
	view, err := queryView(ctx, tx, t.db, t, vid)
	return view, err
}

func (t *tableImpl) GetViewData(ctx context.Context, tx tx.ReadTx, vid common.ViewId) (ret common.TableResult, err error) {
	// TODO
	panic("un impl")
}

func (t *tableImpl) DropTable(ctx context.Context, tx tx.WriteTx) (err error) {

	deleteFromtables := `DELETE FROM tables WHERE table_id = ?`
	if _, err = tx.Exac(deleteFromtables, t.tableId); err != nil {
		return
	}
	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found dataTable")
		return
	}
	dropTable := fmt.Sprintf(`DROP TABLE %s`, dataTable)
	if _, err = tx.Exac(dropTable); err != nil {
		return
	}
	return
}
