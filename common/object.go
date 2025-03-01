package common

import (
	"context"
	"database/sql"
	"paroket/tx"
	"paroket/utils"

	"github.com/rs/xid"
)

type Object interface {
	ObjectId() ObjectId
	Data() []byte
	Update(ctx context.Context, tx tx.WriteTx, data []byte) (err error)
	Delete(ctx context.Context, tx tx.WriteTx) (err error)
}

type object struct {
	db       Database
	objectId ObjectId
	data     []byte
}
type ObjectRelationTable struct {
	Tables utils.JSONMap
}

func NewObject(ctx context.Context, db Database, tx tx.WriteTx) (o Object, err error) {

	guid := xid.New()

	obj := &object{
		db:       db,
		objectId: ObjectId(guid),
		data:     []byte("{}"),
	}
	insertStmt := `INSERT INTO objects 
    (object_id,data)
    VALUES
    (?,jsonb(?))`
	if _, err = tx.Exac(insertStmt, obj.objectId, obj.data); err != nil {
		return
	}

	o = obj
	return
}
func QueryObject(ctx context.Context, db Database, tx tx.ReadTx, oid ObjectId) (obj Object, err error) {
	o := &object{db: db}
	query := `SELECT object_id, json(data) FROM objects WHERE object_id = ?`
	if err = tx.QueryRow(query, oid).Scan(&o.objectId, &o.data); err != nil {
		return
	}
	obj = o
	return
}

func QueryTableObject(ctx context.Context, db Database, rows *sql.Rows) (objList []Object, err error) {
	objList = []Object{}
	for rows.Next() {
		o := &object{db: db}
		if err = rows.Scan(&o.objectId, &o.data); err != nil {
			return
		}
		objList = append(objList, o)
	}
	return
}
func (obj *object) ObjectId() ObjectId {
	return obj.objectId
}

func (obj *object) Data() []byte {
	return obj.data
}

func (obj *object) Update(ctx context.Context, tx tx.WriteTx, data []byte) (err error) {

	doPreUpdateObjectHook(ctx, obj.db, tx, obj)

	updateObjects := `UPDATE objects SET data = jsonb(?) WHERE object_id = ?`
	if _, err = tx.Exac(updateObjects, data, obj.objectId); err != nil {
		return
	}
	obj.data = data
	doAfterUpdateObjectHook(ctx, obj.db, tx, obj)
	return
}

func (obj *object) Delete(ctx context.Context, tx tx.WriteTx) (err error) {
	doPreDeleteObjectHook(ctx, obj.db, tx, obj)
	query := `DELETE FROM objects WHERE object_id = ?`
	if _, err = tx.Exac(query, obj.objectId); err != nil {
		return
	}
	doAfterDeleteObjectHook(ctx, obj.db, tx, obj)
	return
}
