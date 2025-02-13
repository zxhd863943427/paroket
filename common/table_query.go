package common

type TableQuery interface {
	buildStmt() string
}
