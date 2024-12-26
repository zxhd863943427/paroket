package table

type TableId string

type Table struct {
	TableId  TableId
	MetaInfo map[string]interface{}
	Version  int64
}
