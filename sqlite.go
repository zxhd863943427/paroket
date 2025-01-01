package paroket

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"paroket/attribute"
	"paroket/object"
	"paroket/table"
)

type SqliteImpl struct {
	db *sql.DB
}

func testsql() {
	var pk Paroket
	pk = NewSqliteImpl()
	pk.InitDB()
}

func NewSqliteImpl() (s *SqliteImpl) {
	s = &SqliteImpl{
		db: nil,
	}
	return
}

func (s *SqliteImpl) InitDB() (err error) {
	// 创建表
	createTableStmt := `CREATE TABLE IF NOT EXISTS tables (
		table_id BLOB PRIMARY KEY,
    	table_name TEXT NOT NULL,
		meta_info TEXT NOT NULL,
		table_version INTEGER NOT NULL
	);`

	// 创建视图
	createTableViewStmt := `CREATE TABLE IF NOT EXISTS table_views (
		table_id BLOB NOT NULL,
		filter JSONB NOT NULL,
		FOREIGN KEY (table_id) REFERENCES tables(table_id)
	);`

	// 创建对象
	createObjectStmt := `CREATE TABLE IF NOT EXISTS objects (
		object_id BLOB PRIMARY KEY
	);`

	// 创建属性类
	createAttributeClassStmt := `CREATE TABLE IF NOT EXISTS attribute_classes (
		class_id BLOB PRIMARY KEY,
		attribute_name TEXT NOT NULL,
		attribute_type TEXT NOT NULL,
		attribute_meta_info JSONB NOT NULL
	);`

	// 创建表与属性类的关联表
	createTableToAttributeClassStmt := `CREATE TABLE IF NOT EXISTS table_to_attribute_classes (
		table_id BLOB NOT NULL,
		class_id BLOB NOT NULL,
		FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE,
		FOREIGN KEY (class_id) REFERENCES attribute_classes(class_id) ON DELETE CASCADE
	);`

	// 创建对象与属性类的关联表
	createObjectToAttributeClassStmt := `CREATE TABLE IF NOT EXISTS object_to_attribute_classes (
		object_id BLOB NOT NULL,
		class_id BLOB NOT NULL,
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE,
		FOREIGN KEY (class_id) REFERENCES attribute_classes(class_id) ON DELETE CASCADE
	);`

	// 创建对象与表格的关联表
	createObjectToTableStmt := `CREATE TABLE IF NOT EXISTS object_to_tables (
		object_id BLOB NOT NULL,
		table_id BLOB NOT NULL,
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE,
		FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE
	);`

	initStmt := []string{
		createObjectStmt,
		createTableStmt,
		createAttributeClassStmt,
		createTableViewStmt,
		createTableToAttributeClassStmt,
		createObjectToAttributeClassStmt,
		createObjectToTableStmt,
	}
	for _, stmt := range initStmt {
		if _, err = s.db.Exec(stmt); err != nil {
			return
		}
	}
	return
}

// 加载数据库
func (s *SqliteImpl) LoadDB(dbPath string) (err error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return
	}
	// 启用外键支持
	if _, err = db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return
	}
	// 设置WAL模式
	if _, err = db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return
	}
	// 验证外键支持是否启用
	var fkEnabled int
	if err = db.QueryRow("PRAGMA foreign_keys;").Scan(&fkEnabled); err != nil {
		return
	}
	if fkEnabled != 1 {
		return fmt.Errorf("failed to enable foreign key support")
	}
	s.db = db
	return
}

// 添加对象
func (s *SqliteImpl) AddObject(o *object.Object) (obj *object.Object, err error) {
	addObjectStmt := "INSERT INTO objects (object_id) VALUES (?)"
	if _, err = s.db.Exec(addObjectStmt, o.ObjectId); err != nil {
		return
	}
	obj = o
	return
}

// 删除对象
func (s *SqliteImpl) RemoveObject(id object.ObjectId) (obj *object.Object, err error) {
	// 开启事务
	tx, err := s.db.Begin()
	if err != nil {
		return
	}

	// 删除对象
	deleteObjectStmt := "DELETE FROM objects WHERE object_id = ?"
	if _, err = tx.Exec(deleteObjectStmt, id); err != nil {
		tx.Rollback()
		return
	}

	// 提交事务
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return
	}

	obj = &object.Object{ObjectId: id}
	return

}

// 添加属性类
func (s *SqliteImpl) AddAttributeClass(ac *attribute.AttributeClass) (newAc *attribute.AttributeClass, err error) {
	// 插入属性类到属性类表
	tx, err := s.db.Begin()

	if err = ac.InsertClass(tx, "attribute_classes"); err != nil {
		tx.Rollback()
		return
	}
	// 新建属性属性ID——数据表
	if err = ac.CreateDataTable(tx); err != nil {
		tx.Rollback()
		return
	}

	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return
	}

	newAc = &attribute.AttributeClass{
		ClassId:           ac.ClassId,
		AttributeName:     ac.AttributeName,
		AttributeType:     ac.AttributeType,
		AttributeMetaInfo: ac.AttributeMetaInfo,
	}
	return

}

// 删除属性类
func (s *SqliteImpl) RemoveAttributeClass(acid attribute.AttributeClassId) (ac *attribute.AttributeClass, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	ac, err = acid.QueryAttributeClass(tx)
	if err != nil {
		return
	}

	deleteAttributeClassStmt := `DELETE FROM attribute_classes WHERE class_id = ?`
	// 删除索引表和关联表
	err = deleteAttributeIndexAndData(tx, ac)
	if err != nil {
		tx.Rollback()
		return
	}
	if _, err = tx.Exec(deleteAttributeClassStmt, acid); err != nil {
		tx.Rollback()
		return
	}
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return
	}
	return
}

// 删除索引表和关联表
func deleteAttributeIndexAndData(tx *sql.Tx, ac *attribute.AttributeClass) (err error) {
	var deleteIndexStmt, deleteDataStmt string
	deleteIndexStmt = fmt.Sprintf(`DROP TABLE %s`, ac.GetDataIndexName())
	deleteDataStmt = fmt.Sprintf(`DROP TABLE %s`, ac.GetDataTableName())
	deleteStmts := []string{
		deleteIndexStmt,
		deleteDataStmt,
	}

	for _, stmt := range deleteStmts {
		_, err = tx.Exec(stmt)
		if err != nil {
			return
		}
	}
	return
}

// 更新属性类
func (s *SqliteImpl) UpdateAttributeClass(ac *attribute.AttributeClass) (newAc *attribute.AttributeClass, err error) {
	updateAttributeClassStmt := `UPDATE attribute_classes SET attribute_name = ?, attribute_type = ?, attribute_meta_info = ? WHERE class_id = ?`
	if _, err = s.db.Exec(updateAttributeClassStmt, ac.AttributeName, ac.AttributeType, ac.AttributeMetaInfo, ac.ClassId); err != nil {
		return
	}
	newAc = &attribute.AttributeClass{
		ClassId:           ac.ClassId,
		AttributeName:     ac.AttributeName,
		AttributeType:     ac.AttributeType,
		AttributeMetaInfo: ac.AttributeMetaInfo,
	}

	return

}

// 添加表
func (s *SqliteImpl) AddTable(t *table.Table) (nt *table.Table, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// 添加到tables表中
	addTableStmt := `INSERT INTO tables (table_id, table_name, meta_info, table_version) VALUES (?, ?, ?, ?)`

	// 创建table所对应的数据表
	createDataTableStmt := fmt.Sprintf(
		`CREATE TABLE table_%s (
		object_id BLOB PRIMARY KEY, 
		update_time DATETIME,
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
		)`,
		t.TableId.String(),
	)

	if _, err = tx.Exec(addTableStmt, t.TableId, t.TableName, t.MetaInfo, t.Version); err != nil {
		return
	}
	if _, err = tx.Exec(createDataTableStmt, t.TableId); err != nil {
		return
	}

	if err = tx.Commit(); err != nil {
		return
	}
	nt = &table.Table{
		TableId:  t.TableId,
		MetaInfo: t.MetaInfo,
		Version:  t.Version,
	}
	return
}

// 删除表
func (s *SqliteImpl) RemoveTable(tid table.TableId) (t *table.Table, err error) {
	t = &table.Table{}
	tx, err := s.db.Begin()
	if err != nil {
		return
	}

	// 删除数据表
	dropDataTableStmt := fmt.Sprintf(
		`DROP TABLE table_%s`,
		tid.String(),
	)
	// 查询tables表中的记录
	selectStmt := `SELECT table_id, table_name, meta_info, table_version  FROM tables WHERE table_id = ?`

	// 删除tables表中的记录
	deleteTableStmt := `DELETE FROM tables WHERE table_id = ?`

	if _, err = tx.Exec(dropDataTableStmt); err != nil {
		return
	}
	if err = tx.QueryRow(selectStmt, tid).Scan(&t.TableId, &t.TableName, &t.MetaInfo, &t.Version); err != nil {
		return
	}

	if _, err = tx.Exec(deleteTableStmt, tid); err != nil {
		return
	}
	if err = tx.Commit(); err != nil {
		return
	}
	return
}

// 更新表
func (s *SqliteImpl) UpdateTable(t *table.Table) (ot *table.Table, err error) {
	ot = &table.Table{}
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// 查询旧有数据表
	selectStmt := `SELECT table_id, table_name, meta_info, table_version  FROM tables WHERE table_id = ?`
	// 更新数据表
	updateTableStmt := `UPDATE tables SET table_name = ?, meta_info = ?, table_version = ? WHERE table_id = ?`
	if err = tx.QueryRow(selectStmt, t.TableId).Scan(&ot.TableId, &ot.TableName, &ot.MetaInfo, &ot.Version); err != nil {
		return
	}
	if _, err = tx.Exec(updateTableStmt, t.TableName, t.MetaInfo, t.Version, t.TableId); err != nil {
		return
	}
	if err = tx.Commit(); err != nil {
		return
	}
	return

}

// 添加对象到表
func (s *SqliteImpl) AddObjectToTable(tid table.TableId, oid object.ObjectId) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// 插入数据表
	insertTableStmt := fmt.Sprintf(`INSERT INTO table_%s (object_id,update_time) VALUES (?, ?)`, tid)

	// 插入对象与表格的关联表
	insertObjToTableStmt := `INSERT INTO object_to_tables (object_id, table_id) VALUES (?, ?)`

	if _, err = tx.Exec(insertTableStmt, oid, time.Now()); err != nil {
		tx.Rollback()
		return
	}
	if _, err = tx.Exec(insertObjToTableStmt, oid, tid); err != nil {
		tx.Rollback()
		return
	}
	err = tx.Commit()
	return
}

// 从表删除对象
func (s *SqliteImpl) RemoveObjectFromTable(tid table.TableId, oid object.ObjectId) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// 删除数据表中的记录
	deleteTableStmt := fmt.Sprintf(`DELETE FROM table_%s WHERE object_id = ?`, tid.String())
	// 删除关联表的记录
	deleteObjToTableStmt := `DELETE FROM object_to_tables WHERE object_id = ? AND table_id = ?`
	if _, err = tx.Exec(deleteTableStmt, oid); err != nil {
		return
	}
	if _, err = tx.Exec(deleteObjToTableStmt, oid, tid); err != nil {
		return
	}

	err = tx.Commit()
	return
}

// 添加属性类到表
// 当前实现为join表形式，后续可能需要优化为单独的表形式
func (s *SqliteImpl) AddAttributeClassToTable(tid table.TableId, acid attribute.AttributeClassId) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	insertClassToTable := `INSERT INTO table_to_attribute_classes (table_id, class_id) VALUES (?, ?)`
	if _, err = tx.Exec(insertClassToTable, tid, acid); err != nil {
		return
	}
	err = tx.Commit()
	return

}

// 从表删除属性类
func (s *SqliteImpl) RemoveAttributeClassFromTable(tid table.TableId, acid attribute.AttributeClassId) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	deleteTableStmt := `DELETE FROM table_to_attribute_classes WHERE table_id = ? AND class_id = ?`
	if _, err = tx.Exec(deleteTableStmt, tid, acid); err != nil {
		return
	}
	err = tx.Commit()
	return
}

// 添加属性到对象
func (s *SqliteImpl) AddAttributeToObject(oid object.ObjectId, attr attribute.Attribute) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// 插入到属性对象关联表
	insertClassToObjStmt := `INSERT INTO object_to_attribute_classes (object_id, class_id) VALUES (?, ?)`

	if _, err = tx.Exec(insertClassToObjStmt, oid, attr.GetClassId()); err != nil {
		tx.Rollback()
		return
	}

	// 插入到属性和索引表
	if err = attr.InsertData(tx, oid); err != nil {
		tx.Rollback()
		return
	}

	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return
	}

	return
}

// 从对象删除属性类
func (s *SqliteImpl) RemoveAttributeClassFromObject(oid object.ObjectId, acid attribute.AttributeClassId) (err error) {

	// 开启事务
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	// 使用acid查找attributeClass
	ac, err := acid.QueryAttributeClass(tx)
	if err != nil {
		return
	}
	// 从关联表中删除
	deleteAttrFromObjStmt := `DELETE FROM object_to_attribute_classes WHERE object_id = ? AND class_id = ?`
	if _, err = tx.Exec(deleteAttrFromObjStmt, oid, acid); err != nil {
		tx.Rollback()
		return
	}
	// 查找对应的属性
	attr, err := ac.NewAttribute()
	if err != nil {
		return
	}
	err = attr.SearchData(tx, oid)
	if err != nil {
		return
	}

	// 删除属性
	if err = attr.DeleteData(tx); err != nil {
		tx.Rollback()
		return
	}
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return
	}
	return
}

// 获取属性类列表
func (s *SqliteImpl) ListAttributeClasses() (acList []attribute.AttributeClass, err error) {
	queryAttributeClassStmt := `SELECT class_id, attribute_name, attribute_type, attribute_meta_info FROM attribute_classes`
	queryRow, err := s.db.Query(queryAttributeClassStmt)
	if err != nil {
		return
	}
	for queryRow.Next() {
		ac := attribute.AttributeClass{}
		err = queryRow.Scan(&ac.ClassId, &ac.AttributeName, &ac.AttributeType, &ac.AttributeMetaInfo)
		if err != nil {
			return
		}
		acList = append(acList, ac)
	}
	return
}

// 获取表列表
func (s *SqliteImpl) ListTables() (tableList []table.Table, err error) {
	queryTableStmt := `SELECT table_id, table_name, meta_info, table_version FROM tables`
	rows, err := s.db.Query(queryTableStmt)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var t table.Table
		err = rows.Scan(&t.TableId, &t.TableName, &t.MetaInfo, &t.Version)
		if err != nil {
			return
		}
		tableList = append(tableList, t)
	}
	return

}

// 获取属性类关联的对象列表
func (s *SqliteImpl) ListAttributeClassObjects(acid attribute.AttributeClassId) (objList []object.Object, err error) {
	queryObjFromAttrClassStmt := `SELECT object_id FROM object_to_attribute_classes WHERE class_id = ?`
	rows, err := s.db.Query(queryObjFromAttrClassStmt, acid)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		obj := object.Object{}
		err = rows.Scan(&obj.ObjectId)
		if err != nil {
			return
		}
		objList = append(objList, obj)
	}
	return
}

// 获取对象关联的属性列表
func (s *SqliteImpl) ListObjectAttributes(objId object.ObjectId) (attrStoreList []attribute.AttributeStore, err error) {
	tx, err := s.db.Begin()
	queryAttrClassStmt := fmt.Sprintf(`
    SELECT class_id, attribute_name, attribute_type, attribute_meta_info
      FROM attribute_classes
      WHERE class_id in (
        SELECT class_id 
        FROM object_to_attribute_classes 
        WHERE object_id = ?)`)
	attrClassList := []attribute.AttributeClass{}
	classRows, err := tx.Query(queryAttrClassStmt, objId)
	if err != nil {
		return
	}
	defer classRows.Close()
	for classRows.Next() {
		var attrClass = attribute.AttributeClass{}
		err = classRows.Scan(&attrClass.ClassId, &attrClass.AttributeName, &attrClass.AttributeType, &attrClass.AttributeMetaInfo)
		if err != nil {
			return
		}
		attrClassList = append(attrClassList, attrClass)
	}
	for _, attrClass := range attrClassList {
		attributeStore := attribute.AttributeStore{
			AttributeType: attrClass.AttributeType,
		}
		queryAttrStmt := fmt.Sprintf(`SELECT attribute_id, object_id, update_time, data FROM %s WHERE object_id = ?`, attrClass.GetDataTableName())
		err = tx.QueryRow(queryAttrStmt, objId).Scan(
			&attributeStore.AttributeId,
			&attributeStore.ObjectId,
			&attributeStore.UpdateDate,
			&attributeStore.Data,
		)
		if err != nil {
			return
		}
		attrStoreList = append(attrStoreList, attributeStore)
	}
	err = tx.Commit()
	return
}
