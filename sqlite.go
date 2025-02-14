package paroket

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"paroket/common"
	"paroket/tx"
)

type SqliteImpl struct {
	lock *sync.Mutex
	db   *sql.DB
}

func testsql() {
	var pk common.DB
	pk = NewSqliteImpl()
	pk.Open(context.Background(), ":memory:", nil)
}

func NewSqliteImpl() (s *SqliteImpl) {
	s = &SqliteImpl{
		lock: &sync.Mutex{},
		db:   nil,
	}
	return
}

func (s *SqliteImpl) Open(ctx context.Context, dbPath string, config *common.Config) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return
	}
	tx, err := db.BeginTx(ctx, nil)
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
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
		object_id BLOB PRIMARY KEY,
		tables JSONB NOT NULL,
		data JSONB NOT NULL
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

	initStmt := []string{
		createObjectStmt,
		createTableStmt,
		createAttributeClassStmt,
		createTableViewStmt,
		createTableToAttributeClassStmt,
		createObjectToAttributeClassStmt,
	}
	for _, stmt := range initStmt {
		if _, err = tx.Exec(stmt); err != nil {
			return
		}
	}
	// 启用外键支持
	if _, err = tx.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return
	}
	// 设置WAL模式
	if _, err = tx.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return
	}
	// 验证外键支持是否启用
	var fkEnabled int
	if err = tx.QueryRow("PRAGMA foreign_keys;").Scan(&fkEnabled); err != nil {
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
func (s *SqliteImpl) CreateAttributeClass(ctx context.Context, AttrType common.Attribute) (ac common.AttributeClass, err error)

func (s *SqliteImpl) OpenAttributeClass(ctx context.Context, acid common.AttributeClassId) (ac common.AttributeClass, err error)

func (s *SqliteImpl) ListAttributeClass(ctx context.Context) (ac common.AttributeClass, err error)

// Object操作
func (s *SqliteImpl) CreateObject(ctx context.Context) (obj *common.Object, err error)

func (s *SqliteImpl) OpenObject(ctx context.Context, oid common.ObjectId) (obj *common.Object, err error)

// Table 操作
func (s *SqliteImpl) CreateTable(ctx context.Context) (common.Table, error)

func (s *SqliteImpl) OpenTable(ctx context.Context, tid common.TableId) (common.Table, error)

func (s *SqliteImpl) Table(ctx context.Context) (common.Table, error)

// DB操作
func (s *SqliteImpl) ReadTx(ctx context.Context) (rtx tx.ReadTx, err error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return
	}
	rtx = &sqliteReadTx{
		tx: tx,
	}
	return
}

func (s *SqliteImpl) WriteTx(ctx context.Context) (wtx tx.WriteTx, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	wtx = &sqliteWriteTx{
		tx: tx,
	}
	return
}

// 关闭数据库
func (s *SqliteImpl) Close(ctx context.Context) error {
	return s.db.Close()
}
