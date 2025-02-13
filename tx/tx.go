package tx

import "database/sql"

type ReadTx interface {
	Query(stmt string, argList ...any) (*sql.Rows, error)
	QueryRow(stmt string, argList ...any) *sql.Row
	Commit() error
}

type WriteTx interface {
	ReadTx
	Exac(stmt string, argList ...any) (sql.Result, error)
	Rollback() error
}
