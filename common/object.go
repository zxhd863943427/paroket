package common

import (
	"context"
	"database/sql"
	"fmt"
	"paroket/tx"
	"paroket/utils"

	"github.com/rs/xid"
)

type Object interface {
	ObjectId() ObjectId
	Data() []byte
	Update(ctx context.Context, tx tx.WriteTx, data []byte) (err error)
}

type object struct {
	objectId ObjectId
	data     []byte
}
type ObjectRelationTable struct {
	Tables utils.JSONMap
}

func NewObject(ctx context.Context, tx tx.WriteTx) (o Object, err error) {

	guid := xid.New()

	obj := &object{
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
func QueryObject(ctx context.Context, tx tx.ReadTx, oid ObjectId) (obj Object, err error) {
	o := &object{}
	query := `SELECT object_id, json(data) FROM objects WHERE object_id = ?`
	if err = tx.QueryRow(query, oid).Scan(&o.objectId, &o.data); err != nil {
		return
	}
	obj = o
	return
}

func QueryTableObject(ctx context.Context, rows *sql.Rows) (objList []Object, err error) {
	objList = []Object{}
	for rows.Next() {
		o := &object{}
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
	tidList := []TableId{}
	queryTableId := `SELECT table_id FROM object_to_tables WHERE object_id = ?`
	rows, err := tx.Query(queryTableId, obj.objectId)
	if err != nil && err != sql.ErrNoRows {
		return
	}
	for rows.Next() {
		var tableId TableId
		if err = rows.Scan(&tableId); err != nil {
			return
		}
		tidList = append(tidList, tableId)
	}
	for _, tid := range tidList {
		updateRelateTable := fmt.Sprintf(`UPDATE %s SET data = jsonb(?) WHERE object_id = ?`, tid.DataTable())
		if _, err = tx.Exac(updateRelateTable, data, obj.objectId); err != nil {
			return
		}
	}
	updateObjects := `UPDATE objects SET data = jsonb(?) WHERE object_id = ?`
	if _, err = tx.Exac(updateObjects, data, obj.objectId); err != nil {
		return
	}
	obj.data = data
	return
}
