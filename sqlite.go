package paroket

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/mattn/go-sqlite3"

	"paroket/attribute"
	"paroket/common"
	"paroket/tx"
)

type sqliteImpl struct {
	lock     *sync.Mutex
	db       *sql.DB
	acMap    map[common.AttributeClassId]common.AttributeClass
	tableMap map[common.TableId]common.Table
}

func NewSqliteImpl() (s common.DB) {
	s = &sqliteImpl{
		lock:     &sync.Mutex{},
		db:       nil,
		acMap:    map[common.AttributeClassId]common.AttributeClass{},
		tableMap: map[common.TableId]common.Table{},
	}
	return
}

var _ = registerSqliteHook()

func registerSqliteHook() (err error) {
	// pipe = make(chan *sqliteOp)

	sql.Register("sqlite3_extend_by_paroket",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {

				// 注册普通函数
				funcMap := custmFunc()
				for key, impl := range funcMap {
					conn.RegisterFunc(key, impl.impl, impl.pure)
				}
				// 注册统计函数
				aggrFuncMap := custmAggrFunc()
				for key, impl := range aggrFuncMap {
					conn.RegisterAggregator(key, impl.impl, impl.pure)
				}
				return nil
			},
		})
	return
}

func (s *sqliteImpl) Open(ctx context.Context, dbPath string, config *common.Config) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	db, err := sql.Open("sqlite3_extend_by_paroket", dbPath)
	if err != nil {
		return
	}

	// 创建表
	createTableStmt := `CREATE TABLE IF NOT EXISTS tables (
		table_id BLOB PRIMARY KEY,
    	table_name TEXT NOT NULL,
		meta_info TEXT NOT NULL,
		version INTEGER NOT NULL
	);`

	// 创建视图
	createTableViewStmt := `CREATE TABLE IF NOT EXISTS table_views (
		table_id BLOB NOT NULL,
		view_id BLOB NOT NULL,
		query JSONB NOT NULL,
		FOREIGN KEY (table_id) REFERENCES tables(table_id)
	);`

	// 创建对象
	createObjectStmt := `CREATE TABLE IF NOT EXISTS objects (
		key INTEGER PRIMARY KEY,
		object_id BLOB NOT NULL,
		data JSONB NOT NULL,
		unique (object_id)
	);`

	// 创建属性类
	createAttributeClassStmt := `CREATE TABLE IF NOT EXISTS attribute_classes (
		class_id BLOB PRIMARY KEY,
		attribute_name TEXT NOT NULL,
		attribute_key  TEXT NOT NULL,
		attribute_type TEXT NOT NULL,
		attribute_meta_info JSONB NOT NULL,
		unique (attribute_key)
	);`

	// 创建表与属性类的关联表
	createTableToAttributeClassStmt := `CREATE TABLE IF NOT EXISTS table_to_attribute_classes (
		table_id BLOB NOT NULL,
		class_id BLOB NOT NULL,
		FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE,
		FOREIGN KEY (class_id) REFERENCES attribute_classes(class_id) ON DELETE CASCADE
	);
	CREATE INDEX IF NOT EXISTS table_to_attribute_classes_table_id ON table_to_attribute_classes (table_id);
	CREATE INDEX IF NOT EXISTS table_to_attribute_classes_class_id ON table_to_attribute_classes (class_id);`

	// 创建对象与属性类的关联表
	createObjectToAttributeClassStmt := `CREATE TABLE IF NOT EXISTS object_to_attribute_classes (
		object_id BLOB NOT NULL,
		class_id BLOB NOT NULL,
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE,
		FOREIGN KEY (class_id) REFERENCES attribute_classes(class_id) ON DELETE CASCADE
	);
	CREATE INDEX IF NOT EXISTS object_to_attribute_classes_object_id ON object_to_attribute_classes (object_id);
	CREATE INDEX IF NOT EXISTS object_to_attribute_classes_class_id ON object_to_attribute_classes (class_id);`

	// 创建对象与表的关联表
	createObjectTotablesStmt := `CREATE TABLE IF NOT EXISTS object_to_tables (
		object_id BLOB NOT NULL,
		table_id BLOB NOT NULL,
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE,
		FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE
		);
	CREATE INDEX IF NOT EXISTS object_to_tables_object_id ON object_to_tables (object_id, table_id);
	CREATE INDEX IF NOT EXISTS object_to_tables_table_id ON object_to_tables (table_id, object_id);`

	initStmt := []string{
		createObjectStmt,
		createTableStmt,
		createAttributeClassStmt,
		createTableViewStmt,
		createTableToAttributeClassStmt,
		createObjectToAttributeClassStmt,
		createObjectTotablesStmt,
	}
	for _, stmt := range initStmt {
		if _, err = db.Exec(stmt); err != nil {
			return
		}
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
		err = fmt.Errorf("failed to enable foreign key support")
		return
	}
	s.db = db
	return
}

// AttributeClass操作
func (s *sqliteImpl) CreateAttributeClass(ctx context.Context, tx tx.WriteTx, attrType common.AttributeType) (ac common.AttributeClass, err error) {
	return attribute.NewAttributeClass(ctx, s, tx, attrType)
}

func (s *sqliteImpl) OpenAttributeClass(ctx context.Context, tx tx.ReadTx, acid common.AttributeClassId) (ac common.AttributeClass, err error) {
	var ok bool
	if ac, ok = s.acMap[acid]; ok {
		return ac, nil
	}
	ac, err = attribute.QueryAttributeClass(ctx, s, tx, acid)

	if err != nil {
		if errors.Is(err, common.ErrAttributeClassNotFound) {
			err = fmt.Errorf("acid %v not found:%w", acid, err)
		}

		return
	}
	s.acMap[acid] = ac
	return
}

func (s *sqliteImpl) ListAttributeClass(ctx context.Context, tx tx.ReadTx) (acList []common.AttributeClass, err error) {
	acidList := []common.AttributeClassId{}
	acList = []common.AttributeClass{}
	var rows *sql.Rows
	queryClassIdStmt := `
	SELECT class_id FROM attribute_classes`
	rows, err = tx.Query(queryClassIdStmt)
	if err != nil {
		return
	}
	for rows.Next() {
		var acid common.AttributeClassId
		if err = rows.Scan(&acid); err != nil {
			return
		}
		acidList = append(acidList, acid)
	}
	if err != nil {
		return
	}
	for _, acid := range acidList {
		var ac common.AttributeClass
		ac, err = s.OpenAttributeClass(ctx, tx, acid)
		if err != nil {
			return
		}
		acList = append(acList, ac)
	}

	return
}

func (s *sqliteImpl) DeleteAttributeClass(ctx context.Context, tx tx.WriteTx, acid common.AttributeClassId) (err error) {
	ac, err := s.OpenAttributeClass(ctx, tx, acid)
	if err != nil {
		return
	}
	err = ac.Drop(ctx, tx)
	return
}

// Object操作
func (s *sqliteImpl) CreateObject(ctx context.Context, tx tx.WriteTx) (obj common.Object, err error) {

	obj, err = common.NewObject(ctx, s, tx)
	if err != nil {
		return
	}
	return
}

func (s *sqliteImpl) OpenObject(ctx context.Context, tx tx.ReadTx, oid common.ObjectId) (obj common.Object, err error) {

	obj, err = common.QueryObject(ctx, s, tx, oid)
	if err != nil {
		return
	}
	return
}

func (s *sqliteImpl) DeleteObject(ctx context.Context, tx tx.WriteTx, oid common.ObjectId) (err error) {
	obj, err := s.OpenObject(ctx, tx, oid)
	if err != nil {
		return
	}
	err = obj.Delete(ctx, tx)
	if err != nil {
		return
	}
	return
}

// Table 操作
func (s *sqliteImpl) CreateTable(ctx context.Context, tx tx.WriteTx) (table common.Table, err error) {
	table, err = newTable(ctx, s, tx)
	return
}

func (s *sqliteImpl) OpenTable(ctx context.Context, tx tx.ReadTx, tid common.TableId) (table common.Table, err error) {
	if table, ok := s.tableMap[tid]; ok {
		return table, nil
	}
	table, err = queryTable(ctx, s, tx, tid)
	if err != nil {
		return
	}
	s.tableMap[tid] = table
	return
}

func (s *sqliteImpl) DeleteTable(ctx context.Context, tx tx.WriteTx, tid common.TableId) (err error) {
	table, err := queryTable(ctx, s, tx, tid)
	if err != nil {
		return
	}
	if err = table.DropTable(ctx, tx); err != nil {
		return
	}
	return
}

// DB操作
func (s *sqliteImpl) ReadTx(ctx context.Context) (rtx tx.ReadTx, err error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return
	}
	rtx = &sqliteReadTx{
		ctx: ctx,
		tx:  tx,
	}
	return
}

func (s *sqliteImpl) WriteTx(ctx context.Context) (wtx tx.WriteTx, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	wtx = &sqliteWriteTx{
		ctx: ctx,
		tx:  tx,
	}
	return
}

// 关闭数据库
func (s *sqliteImpl) Close(ctx context.Context) error {
	return s.db.Close()
}
