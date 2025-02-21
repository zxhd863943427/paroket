package paroket

import (
	"context"
	"database/sql"
)

type sqliteReadTx struct {
	ctx context.Context
	tx  *sql.Tx
}

type sqliteWriteTx struct {
	ctx context.Context
	tx  *sql.Tx
}

func (tx *sqliteReadTx) Query(stmt string, argList ...any) (*sql.Rows, error) {
	return tx.tx.QueryContext(tx.ctx, stmt, argList...)
}
func (tx *sqliteReadTx) QueryRow(stmt string, argList ...any) *sql.Row {
	return tx.tx.QueryRowContext(tx.ctx, stmt, argList...)
}
func (tx *sqliteReadTx) Prepare(stmt string) (*sql.Stmt, error) {
	return tx.tx.PrepareContext(tx.ctx, stmt)
}
func (tx *sqliteReadTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *sqliteWriteTx) Query(stmt string, argList ...any) (*sql.Rows, error) {
	return tx.tx.QueryContext(tx.ctx, stmt, argList...)
}

func (tx *sqliteWriteTx) QueryRow(stmt string, argList ...any) *sql.Row {
	return tx.tx.QueryRowContext(tx.ctx, stmt, argList...)
}

func (tx *sqliteWriteTx) Prepare(stmt string) (*sql.Stmt, error) {
	return tx.tx.PrepareContext(tx.ctx, stmt)
}

func (tx *sqliteWriteTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *sqliteWriteTx) Exac(stmt string, argList ...any) (sql.Result, error) {
	return tx.tx.ExecContext(tx.ctx, stmt, argList...)
}

func (tx *sqliteWriteTx) Rollback() error {
	return tx.tx.Rollback()
}
