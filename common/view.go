package common

type View interface {
}

type view struct {
	TableId TableId
	Filter  string
}
