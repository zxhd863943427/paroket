package paroket

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/mattn/go-sqlite3"
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

type sqliteOp struct {
	op    int
	db    string
	table string
	rowid int64
}

func registerSqliteHook() (pipe chan *sqliteOp) {
	pipe = make(chan *sqliteOp)

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
				// 注册update hook
				conn.RegisterUpdateHook(func(op int, db string, table string, rowid int64) {
					o := &sqliteOp{
						op:    op,
						db:    db,
						table: table,
						rowid: rowid,
					}
					go func() {
						pipe <- o
					}()
				})
				return nil
			},
		})
	return
}

func updateTableHook(s *SqliteImpl, ctx context.Context, pipe chan *sqliteOp) {
	for {
		op := <-pipe
		switch op.op {
		case sqlite3.SQLITE_UPDATE:
			if op.table == "objects" {
				_, err := tryGetTx(s, ctx)
				if err != nil {
					continue
				}
				fmt.Println(op)
			}
		}
	}
}

func tryGetTx(s *SqliteImpl, ctx context.Context) (tx tx.WriteTx, err error) {
	cnt := 10
	for i := 0; i < cnt; i++ {
		tx, err = s.WriteTx(ctx)
		if err != nil {
			break
		}
	}
	return
}

func (s *SqliteImpl) Open(ctx context.Context, dbPath string, config *common.Config) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	pipe := registerSqliteHook()
	db, err := sql.Open("sqlite3_extend_by_paroket", dbPath)
	go updateTableHook(s, ctx, pipe)
	if err != nil {
		return
	}

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
func (s *SqliteImpl) CreateAttributeClass(ctx context.Context, AttrType common.AttributeType) (ac common.AttributeClass, err error) {
	// TODO
	panic("un impl")
}

func (s *SqliteImpl) OpenAttributeClass(ctx context.Context, acid common.AttributeClassId) (ac common.AttributeClass, err error) {
	// TODO
	panic("un impl")
}

func (s *SqliteImpl) ListAttributeClass(ctx context.Context) (ac common.AttributeClass, err error) {
	// TODO
	panic("un impl")
}

func (s *SqliteImpl) DeleteAttributeClass(ctx context.Context) (err error) {
	// TODO
	panic("un impl")
}

// Object操作
func (s *SqliteImpl) CreateObject(ctx context.Context) (obj *common.Object, err error) {

	obj, err = common.NewObject()
	if err != nil {
		return
	}
	tx, err := s.WriteTx(ctx)
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
	insertStmt := `INSERT INTO objects 
    (object_id,tables,data)
    VALUES
    (?,'{}',?)`
	if _, err = tx.Exac(insertStmt, obj.ObjectId, obj.Data); err != nil {
		return
	}
	return
}

func (s *SqliteImpl) OpenObject(ctx context.Context, oid common.ObjectId) (obj *common.Object, err error) {
	// TODO
	panic("un impl")
}

func (s *SqliteImpl) DeleteObject(ctx context.Context, oid common.ObjectId) (err error) {
	// TODO
	panic("un impl")
}

// Table 操作
func (s *SqliteImpl) CreateTable(ctx context.Context) (table common.Table, err error) {
	// TODO
	panic("un impl")
}

func (s *SqliteImpl) OpenTable(ctx context.Context, tid common.TableId) (table common.Table, err error) {
	// TODO
	panic("un impl")
}

func (s *SqliteImpl) Table(ctx context.Context, tid common.TableId) (table common.Table, err error) {
	// TODO
	panic("un impl")
}

func (s *SqliteImpl) DeleteTable(ctx context.Context, tid common.TableId) error {
	// TODO
	panic("un impl")
}

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
	s.lock.Lock()
	defer s.lock.Unlock()
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
