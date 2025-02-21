package paroket

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"

	"paroket/attribute"
	"paroket/common"
	"paroket/tx"
)

type SqliteImpl struct {
	lock     *sync.Mutex
	db       *sql.DB
	acMap    map[common.AttributeClassId]common.AttributeClass
	tableMap map[common.TableId]common.Table
}

func testsql() {
	var pk common.DB
	pk = NewSqliteImpl()
	pk.Open(context.Background(), ":memory:", nil)
}

func NewSqliteImpl() (s *SqliteImpl) {
	s = &SqliteImpl{
		lock:     &sync.Mutex{},
		db:       nil,
		acMap:    map[common.AttributeClassId]common.AttributeClass{},
		tableMap: map[common.TableId]common.Table{},
	}
	return
}

type sqliteOp struct {
	op    int
	db    string
	table string
	rowid int64
}

var pipe = registerSqliteHook()

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
		version INTEGER NOT NULL
	);`

	// 创建视图
	createTableViewStmt := `CREATE TABLE IF NOT EXISTS table_views (
		table_id BLOB NOT NULL,
		filter JSONB NOT NULL,
		FOREIGN KEY (table_id) REFERENCES tables(table_id)
	);`

	// 创建对象
	createObjectStmt := `CREATE TABLE IF NOT EXISTS objects (
		key INTEGER PRIMARY KEY,
		object_id BLOB NOT NULL,
		tables JSONB NOT NULL,
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
func (s *SqliteImpl) CreateAttributeClass(ctx context.Context, attrType common.AttributeType) (ac common.AttributeClass, err error) {
	return attribute.NewAttributeClass(ctx, s, attrType)
}

func (s *SqliteImpl) OpenAttributeClass(ctx context.Context, acid common.AttributeClassId) (ac common.AttributeClass, err error) {
	var ok bool
	if ac, ok = s.acMap[acid]; ok {
		return ac, nil
	}
	ac, err = attribute.QueryAttributeClass(ctx, s, acid)

	if err != nil {
		if errors.Is(err, common.ErrAttributeClassNotFound) {
			err = fmt.Errorf("acid %v not found:%w", acid, err)
		}

		return
	}
	s.acMap[acid] = ac
	return
}

func (s *SqliteImpl) ListAttributeClass(ctx context.Context) (acList []common.AttributeClass, err error) {
	acidList := []common.AttributeClassId{}
	acList = []common.AttributeClass{}
	func() {
		var tx tx.ReadTx
		var rows *sql.Rows

		tx, err = s.ReadTx(ctx)
		if err != nil {
			return
		}
		defer tx.Commit()
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
	}()
	if err != nil {
		return
	}
	for _, acid := range acidList {
		var ac common.AttributeClass
		ac, err = attribute.QueryAttributeClass(ctx, s, acid)
		if err != nil {
			return
		}
		acList = append(acList, ac)
	}

	return
}

func (s *SqliteImpl) DeleteAttributeClass(ctx context.Context, acid common.AttributeClassId) (err error) {
	ac, err := s.OpenAttributeClass(ctx, acid)
	if err != nil {
		return
	}
	err = ac.Drop(ctx)
	return
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
    (?,'{}',jsonb(?))`
	if _, err = tx.Exac(insertStmt, obj.ObjectId, obj.Data); err != nil {
		return
	}
	return
}

func (s *SqliteImpl) OpenObject(ctx context.Context, oid common.ObjectId) (obj *common.Object, err error) {
	tx, err := s.ReadTx(ctx)
	if err != nil {
		return
	}
	defer tx.Commit()
	obj = &common.Object{}
	query := `SELECT object_id ,data FROM objects WHERE object_id = ?`
	if err = tx.QueryRow(query, oid).Scan(&obj.ObjectId, &obj.Data); err != nil {
		return
	}
	return
}

func (s *SqliteImpl) DeleteObject(ctx context.Context, oid common.ObjectId) (err error) {
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
	query := `DELETE FROM objects WHERE object_id = ?`
	if _, err = tx.Exac(query, oid); err != nil {
		return
	}
	return
}

// Table 操作
func (s *SqliteImpl) CreateTable(ctx context.Context) (table common.Table, err error) {
	table, err = NewTable(ctx, s)
	return
}

func (s *SqliteImpl) OpenTable(ctx context.Context, tid common.TableId) (table common.Table, err error) {
	table, err = QueryTable(ctx, s, tid)
	return
}

func (s *SqliteImpl) DeleteTable(ctx context.Context, tid common.TableId) (err error) {
	table, err := QueryTable(ctx, s, tid)
	if err != nil {
		return
	}
	if err = table.DropTable(ctx); err != nil {
		return
	}
	return
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
		ctx: ctx,
		tx:  tx,
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
		ctx: ctx,
		tx:  tx,
	}
	return
}

// 关闭数据库
func (s *SqliteImpl) Close(ctx context.Context) error {
	return s.db.Close()
}
