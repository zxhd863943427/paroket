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
)

const createFtsTriggerTemplate = `CREATE TRIGGER %s AFTER UPDATE OF data ON %s 
BEGIN
UPDATE %s SET idx =%s
WHERE object_id = NEW.object_id;
END;`

type tableImpl struct {
	lock      *sync.Mutex
	db        common.DB
	tableId   common.TableId
	tableName string
	fields    []common.AttributeClassId
	metaInfo  utils.JSONMap
	version   int64
}

func NewTable(ctx context.Context, db common.DB) (table common.Table, err error) {
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
	tx, err := db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	createTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		object_id BLOB PRIMARY KEY,
    	data JSONB,
		idx BLOB DEFAULT '',
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
	);
	CREATE TRIGGER IF NOT EXISTS insert_%s 
	AFTER INSERT ON %s
	FOR EACH ROW
	BEGIN
		UPDATE %s SET data = 
		(SELECT data
		FROM objects
		WHERE objects.object_id = NEW.object_id LIMIT 1)
		WHERE object_id = NEW.object_id;
	END;
	`, dataTable, dataTable, dataTable, dataTable)
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
	createFtsTrigger := fmt.Sprintf(
		createFtsTriggerTemplate,
		ftsTrigger, dataTable, dataTable, `''`,
	)
	_, err = tx.Exac(createFtsTrigger)
	if err != nil {
		return
	}
	return
}

func QueryTable(ctx context.Context, db common.DB, tid common.TableId) (table common.Table, err error) {
	tx, err := db.ReadTx(ctx)
	if err != nil {
		return
	}
	defer tx.Commit()
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

func (t *tableImpl) Set(ctx context.Context, v utils.JSONMap) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	oldName := t.tableName
	oldMetaInfo := utils.JSONMap{}
	for key := range t.metaInfo {
		oldMetaInfo[key] = t.metaInfo[key]
	}
	tx, err := t.db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			t.tableName = oldName
			t.metaInfo = oldMetaInfo
		} else {
			tx.Commit()
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

func (t *tableImpl) FindId(ctx context.Context, oidList ...common.ObjectId) (objList []*common.Object, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	tx, err := t.db.ReadTx(ctx)
	if err != nil {
		return
	}
	defer tx.Commit()
	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found tablekey")
	}
	stmt := fmt.Sprintf(`SELECT object_id,data FROM %s WHERE object_id = ?`, dataTable)
	objList = []*common.Object{}
	for _, oid := range oidList {
		obj := &common.Object{}
		err = tx.QueryRow(stmt, oid).Scan(&obj.ObjectId, &obj.Data)
		if err != nil {
			return
		}
		objList = append(objList, obj)
	}
	return
}

func (t *tableImpl) Insert(ctx context.Context, oidList ...common.ObjectId) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	tx, err := t.db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found tablekey")
	}
	stmt := fmt.Sprintf(`
	INSERT INTO %s
    	(object_id)
  	VALUES
    	(?);`, dataTable)
	updateStmt := fmt.Sprintf(`
	UPDATE objects SET tables = jsonb_set(tables,'$."%s"',1) 
	WHERE object_id = ?;`, dataTable)
	for _, oid := range oidList {
		if _, err = tx.Exac(stmt, oid); err != nil {
			return
		}
		if _, err = tx.Exac(updateStmt, oid); err != nil {
			return
		}
	}
	return
}

func (t *tableImpl) Delete(ctx context.Context, oidList ...common.ObjectId) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	tx, err := t.db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found dataTable")
	}
	stmt := fmt.Sprintf(`DELETE FROM %s WHERE object_id = ?;`, dataTable)
	updateStmt := fmt.Sprintf(`
	UPDATE objects SET tables = jsonb_remove(tables,'$."%s"') 
	WHERE object_id = ?;`, dataTable)
	for _, oid := range oidList {
		if _, err = tx.Exac(stmt, oid); err != nil {
			return
		}
		if _, err = tx.Exac(updateStmt, oid); err != nil {
			return
		}
	}
	return
}

func (t *tableImpl) AddAttributeClass(ctx context.Context, ac common.AttributeClass) (err error) {
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
		oldAc, err = t.db.OpenAttributeClass(ctx, acid)
		if err != nil {
			return
		}
		acList = append(acList, oldAc)
	}

	tx, err := t.db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
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
	metaInfo, err := ac.GetMetaInfo(ctx)
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
	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found dataTable")
	}
	// 构建索引表达式
	searchIdxFormulaBuffer := &bytes.Buffer{}
	lenAc := len(acList)
	for idx, acItem := range acList {
		var metaInfo utils.JSONMap
		metaInfo, err = acItem.GetMetaInfo(ctx)
		if err != nil {
			return
		}
		jsonValuePath, ok := metaInfo["json_value_path"]
		if !ok {
			err = fmt.Errorf("add attributeclass to table failed:can not get json path")
			return
		}
		searchIdxFormulaBuffer.WriteString(
			fmt.Sprintf(
				`COALESCE(%s.data ->>'%s','')`,
				dataTable, jsonValuePath,
			),
		)
		if idx != lenAc-1 {
			searchIdxFormulaBuffer.WriteString(" || X'E2808B' || ")
		}
	}
	searchIdxFormula := searchIdxFormulaBuffer.String()

	// 删除旧有trigger
	triggerName, ok := t.metaInfo["fts_trigger"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found fts_trigger")
	}
	deleteTrigger := fmt.Sprintf("DROP TRIGGER  %s", triggerName)
	if _, err = tx.Exac(deleteTrigger); err != nil {
		return
	}

	// 更新fts索引

	updateFtsIdx := fmt.Sprintf(`
		UPDATE %s SET idx = %s`, dataTable, searchIdxFormula)
	if _, err = tx.Exac(updateFtsIdx); err != nil {
		return
	}

	// 重建trigger
	createFtsTrigger := fmt.Sprintf(
		createFtsTriggerTemplate,
		triggerName, dataTable, dataTable, searchIdxFormula,
	)

	if _, err = tx.Exac(createFtsTrigger); err != nil {
		return
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

func (t *tableImpl) DeleteAttributeClass(ctx context.Context, ac common.AttributeClass) (err error) {

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
		oldAc, err = t.db.OpenAttributeClass(ctx, acid)
		if err != nil {
			return
		}
		acList = append(acList, oldAc)
	}

	tx, err := t.db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// 删除属性关联表
	insertFields := `
	DELETE FROM table_to_attribute_classes
	WHRER table_id = ? AND class_id = ?;`
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

func (t *tableImpl) Find(ctx context.Context, query common.TableQuery) ([]*common.Object, error) {
	// TODO
	panic("un impl")
}

func (t *tableImpl) NewView(ctx context.Context) (common.View, error) {
	// TODO
	panic("un impl")
}

func (t *tableImpl) GetViewData(ctx context.Context, view common.View, config common.QueryConfig) ([][]common.Attribute, error) {
	// TODO
	panic("un impl")
}

func (t *tableImpl) DropTable(ctx context.Context) (err error) {

	tx, err := t.db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	deleteFromtables := `DELETE FROM tables WHERE table_id = ?`
	if _, err = tx.Exac(deleteFromtables, t.tableId); err != nil {
		return
	}
	dataTable, ok := t.metaInfo["data_table"].(string)
	if !ok {
		err = fmt.Errorf("table metainfo not found dataTable")
	}
	dropTable := fmt.Sprintf(`DROP TABLE %s`, dataTable)
	if _, err = tx.Exac(dropTable); err != nil {
		return
	}
	return
}
