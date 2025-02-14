package paroket

import "database/sql"

type sqliteReadTx struct {
	tx *sql.Tx
}

type sqliteWriteTx struct {
	tx *sql.Tx
}

func (tx *sqliteReadTx) Query(stmt string, argList ...any) (*sql.Rows, error) {
	return tx.tx.Query(stmt, argList...)
}
func (tx *sqliteReadTx) QueryRow(stmt string, argList ...any) *sql.Row {
	return tx.tx.QueryRow(stmt, argList...)
}
func (tx *sqliteReadTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *sqliteWriteTx) Query(stmt string, argList ...any) (*sql.Rows, error) {
	return tx.tx.Query(stmt, argList...)
}

func (tx *sqliteWriteTx) QueryRow(stmt string, argList ...any) *sql.Row {
	return tx.tx.QueryRow(stmt, argList...)
}

func (tx *sqliteWriteTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *sqliteWriteTx) Exac(stmt string, argList ...any) (sql.Result, error) {
	return tx.tx.Exec(stmt, argList...)
}

func (tx *sqliteWriteTx) Rollback() error {
	return tx.tx.Rollback()
}
