package attribute

import "fmt"

func genTableNameByTypeAndID(attrType string, cid AttributeClassId) string {
	return fmt.Sprintf(
		`%s_%s`,
		attrType,
		cid.String())
}

func genIndexNameByTypeAndID(attrType string, cid AttributeClassId) string {
	return fmt.Sprintf(
		`%s_%s_idx`,
		attrType,
		cid.String())
}
