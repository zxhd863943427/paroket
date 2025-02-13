package paroket

import (
	"context"
	"fmt"
	"paroket/common"
	"paroket/utils"
	"sync"
)

type tableImpl struct {
	lock      *sync.Mutex
	db        common.DB
	tableId   common.TableId
	tableName string
	fields    []common.AttributeClassId
	metaInfo  *utils.JSONMap
	version   int64
}

func NewTable(ctx context.Context, db common.DB) (table common.Table, err error) {
	id, err := common.NewTableId()
	if err != nil {
		return
	}
	t := &tableImpl{
		lock:      &sync.Mutex{},
		db:        db,
		tableId:   id,
		tableName: "untitled",
		metaInfo:  &utils.JSONMap{},
		version:   0,
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

	tableName := fmt.Sprintf("table_%s", id.String())
	createTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		object_id BLOB PRIMARY KEY,
    data JSONB,
		idx BLOB DEFAULT '',
		FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
	);
	CREATE TRIGGER IF NOT EXISTS insert_%s 
	AFTER INSERT ON tables
	FOR EACH ROW
	BEGIN
		INSERT INTO %s (object_id, data)
		SELECT NEW.object_id, data
		FROM objects
		WHERE object_id = NEW.object_id;
	END;
	`, tableName, tableName, tableName)
	_, err = tx.Exac(createTable)
	if err != nil {
		return
	}

	insertTable := `
  INSERT INTO tables 
  (object_id,table_name,meta_info,version) 
  VALUES
  (?,?,?,?)`
	_, err = tx.Exac(insertTable, t.tableId, t.tableName, t.metaInfo, t.version)
	if err != nil {
		return
	}
	return
}

func (t *tableImpl) sqlTable() string {
	return fmt.Sprintf(`table_%s`, t.tableId.String())
}

func (t *tableImpl) TableId() common.TableId {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.tableId
}

func (t *tableImpl) FindId(ctx context.Context, oidList ...common.ObjectId) (objList []*common.Object, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	tx, err := t.db.ReadTx(ctx)
	if err != nil {
		return
	}
	defer tx.Commit()
	stmt := fmt.Sprintf(`SELECT object_id,data FROM %s WHERE object_id = ?`, t.sqlTable())
	objList = []*common.Object{}
	for _, oid := range oidList {
		obj := &common.Object{}
		err = tx.QueryRow(stmt, oid).Scan(&obj.ObjectId, &obj.Value)
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
	stmt := fmt.Sprintf(`
	INSERT INSERT %s
    	(object_id)
  	VALUES
    	(?);`, t.sqlTable())
	updateStmt := fmt.Sprintf(`
	UPDATE objects SET tables = jsonb_set(tables,'$."%s"',1) 
	WHERE object_id = ?;`, t.sqlTable())
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

func (t *tableImpl) Delete(ctx context.Context, oidList ...common.ObjectId) error

func (t *tableImpl) AddAttributeClass(ctx context.Context, ac common.AttributeClass) (err error) {
	// TODO
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
	// 插入属性-表关联表

	// 修改索引触发器并更新全部索引

	return
}

func (t *tableImpl) DeleteAttributeClass(ctx context.Context, ac common.AttributeClass) error

func (t *tableImpl) Find(ctx context.Context, query common.TableQuery) ([]*common.Object, error)

func (t *tableImpl) NewView(ctx context.Context) (common.View, error)

func (t *tableImpl) GetViewData(ctx context.Context, view common.View, config common.QueryConfig) ([][]common.Attribute, error)

func (t *tableImpl) DropTable(ctx context.Context) error
