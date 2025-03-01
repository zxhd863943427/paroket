package common

import (
	"context"
	"fmt"
	"paroket/tx"
)

type HookObjectOp func(ctx context.Context, db Database, tx tx.WriteTx, obj Object) (err error)

var preUpdateObjectHook = []HookObjectOp{}
var afterUpdateObjectHook = []HookObjectOp{}

var preDeleteObjectHook = []HookObjectOp{}
var afterDeleteObjectHook = []HookObjectOp{}

func RegisterPreUpdateObjectHook(hook HookObjectOp) (err error) {
	preUpdateObjectHook = append(preUpdateObjectHook, hook)
	return
}

func RegisterAfterUpdateObjectHook(hook HookObjectOp) (err error) {
	afterUpdateObjectHook = append(afterUpdateObjectHook, hook)
	return
}

func RegisterPreDeleteObjectHook(hook HookObjectOp) (err error) {
	preDeleteObjectHook = append(preDeleteObjectHook, hook)
	return
}

func RegisterAfterDeleteObjectHook(hook HookObjectOp) (err error) {
	afterDeleteObjectHook = append(afterDeleteObjectHook, hook)
	return
}

func doHook(ctx context.Context, db Database, tx tx.WriteTx, objectHook []HookObjectOp, obj Object) (err error) {
	for _, hook := range objectHook {
		nerr := hook(ctx, db, tx, obj)
		if nerr != nil {
			if err == nil {
				err = nerr
			} else {
				err = fmt.Errorf("%w:%w", err, nerr)
			}
		}
	}
	return
}

func doPreUpdateObjectHook(ctx context.Context, db Database, tx tx.WriteTx, obj Object) (err error) {
	doHook(ctx, db, tx, preUpdateObjectHook, obj)
	return
}

func doAfterUpdateObjectHook(ctx context.Context, db Database, tx tx.WriteTx, obj Object) (err error) {
	doHook(ctx, db, tx, afterUpdateObjectHook, obj)
	return
}

func doPreDeleteObjectHook(ctx context.Context, db Database, tx tx.WriteTx, obj Object) (err error) {
	doHook(ctx, db, tx, preDeleteObjectHook, obj)
	return
}

func doAfterDeleteObjectHook(ctx context.Context, db Database, tx tx.WriteTx, obj Object) (err error) {
	doHook(ctx, db, tx, afterDeleteObjectHook, obj)
	return
}
