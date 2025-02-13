package attribute

import (
	"database/sql"
	"fmt"
	"paroket/common"
)

// AttributeClass的具体实现接口
type attributeClassImpl interface {
	NewAttribute() (attr common.Attribute, err error)                       //创建Attrbute
	SearchByID(tx *sql.Tx, objId common.ObjectId) (common.Attribute, error) //根据ObjectId搜索属性
	CreateDataTable(tx *sql.Tx) error                                       // 创建AttributeClass相关的表
	GetDataTableName() string                                               //获取数据表名称
	GetDataIndexName() string                                               //获取索引表名称
	BuildQuery(v map[string]interface{}) (string, error)                    //构建查询
	BuildSort(v map[string]interface{}) (string, error)                     //构建排序
}

func genTableNameByTypeAndID(attrType string, cid common.AttributeClassId) string {
	return fmt.Sprintf(
		`%s_%s`,
		attrType,
		cid.String())
}

func genIndexNameByTypeAndID(attrType string, cid common.AttributeClassId) string {
	return fmt.Sprintf(
		`%s_%s_idx`,
		attrType,
		cid.String())
}
