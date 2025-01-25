package attribute

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"paroket/object"
	"paroket/utils"
	"strings"
	"time"

	"github.com/google/uuid"
)

type AttributeId uuid.UUID

type AttributeClassId uuid.UUID

// Scan 实现 sql.Scanner 接口
func (id *AttributeClassId) Scan(value interface{}) error {
	return (*uuid.UUID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id AttributeClassId) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}

// Scan 实现 sql.Scanner 接口
func (id *AttributeId) Scan(value interface{}) error {
	return (*uuid.UUID)(id).Scan(value)
}

// Value 实现 driver.Valuer 接口
func (id AttributeId) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}

type attributeClassFieldMap struct{}

func (am attributeClassFieldMap) ClassId() string           { return `class_id` }
func (am attributeClassFieldMap) AttributeName() string     { return `attribute_name` }
func (am attributeClassFieldMap) AttributeType() string     { return `attribute_type` }
func (am attributeClassFieldMap) AttributeMetaInfo() string { return `attribute_meta_info` }

var AttributeClassFieldMap = attributeClassFieldMap{}

func AttributeClassField() string {
	return fmt.Sprintf(
		` %s, %s, %s, %s `,
		AttributeClassFieldMap.ClassId(),
		AttributeClassFieldMap.AttributeName(),
		AttributeClassFieldMap.AttributeType(),
		AttributeClassFieldMap.AttributeMetaInfo(),
	)
}

func InsertField() string {
	return `(?, ?, ?, ?)`
}

func NewAttributeClassId() (AttributeClassId, error) {
	uuid, err := uuid.NewV7()
	return AttributeClassId(uuid), err
}

func NewAttributeId() (AttributeId, error) {
	uuid, err := uuid.NewV7()
	return AttributeId(uuid), err
}

// AttributeClass的具体实现接口
type AttributeClassImpl interface {
	NewAttribute() (attr Attribute, err error)                       //创建Attrbute
	SearchByID(tx *sql.Tx, objId object.ObjectId) (Attribute, error) //根据ObjectId搜索属性
	CreateDataTableStmt() (dataTable, indexTable, execIndex string)  // 获取创建data的语句
	GetDataTableName() string                                        //获取数据表名称
	GetDataIndexName() string                                        //获取索引表名称
	BuildQuery(v map[string]interface{}) (string, error)             //构建查询
	BuildSort(v map[string]interface{}) (string, error)              //构建排序
}

// 公用实现
type AttributeClass struct {
	ClassId           AttributeClassId
	AttributeName     string
	AttributeType     string
	AttributeMetaInfo utils.JSONMap
	Impl              AttributeClassImpl
}

func NewAttributeClass(attributbuteType string) (ac *AttributeClass, err error) {
	uuid, err := uuid.NewV7()
	if err != nil {
		return
	}
	cid := AttributeClassId(uuid)
	switch attributbuteType {
	case AttributeTypeText:
		ac = &AttributeClass{
			ClassId:           cid,
			AttributeName:     "untitled",
			AttributeType:     AttributeTypeText,
			AttributeMetaInfo: map[string]interface{}{},
		}
		ac.Impl = &TextAttributeClass{AttributeClass: ac}
	default:
		err = fmt.Errorf("unsupport attribute type of %s", attributbuteType)
		return
	}
	// ac = &AttributeClass{
	// 	ClassId:           AttributeClassId(uuid),
	// 	AttributeName:     "untitled",
	// 	AttributeType:     attributbuteType,
	// 	AttributeMetaInfo: map[string]interface{}{},
	// }

	return
}

func (ac *AttributeClass) SearchByID(tx *sql.Tx, objId object.ObjectId) (attr Attribute, err error) {
	return ac.Impl.SearchByID(tx, objId)
}

func (ac *AttributeClass) GetDataTableName() string {
	return ac.Impl.GetDataTableName()
}

func (ac *AttributeClass) GetDataIndexName() string {
	return ac.Impl.GetDataIndexName()
}

func (ac *AttributeClass) NewAttribute() (attr Attribute, err error) {
	return ac.Impl.NewAttribute()
}

func (ac *AttributeClass) ScanRow(row *sql.Row) (err error) {
	if ac == nil {
		return fmt.Errorf(`nil of AttributeClass`)
	}
	err = row.Scan(&ac.ClassId, &ac.AttributeName, &ac.AttributeType, &ac.AttributeMetaInfo)
	return
}

func (ac *AttributeClass) InsertClass(tx *sql.Tx, tableName string) (err error) {
	addAttributeClassStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, AttributeClassField(), InsertField())
	_, err = tx.Exec(addAttributeClassStmt, ac.ClassId, ac.AttributeName, ac.AttributeType, ac.AttributeMetaInfo)
	return
}

func (ac *AttributeClass) CreateDataTable(tx *sql.Tx) (err error) {
	dataStmt, indexStmt, execIdxStmt := ac.Impl.CreateDataTableStmt()
	stmts := []string{dataStmt, indexStmt, execIdxStmt}
	for _, stmt := range stmts {
		_, err = tx.Exec(stmt)
		if err != nil {
			return
		}
	}
	return
}

func (acid AttributeClassId) String() string {
	uuid := uuid.UUID(acid)
	return strings.ReplaceAll(uuid.String(), "-", "_")
}

func (acid AttributeClassId) QueryAttributeClass(tx *sql.Tx) (ac *AttributeClass, err error) {
	ac = &AttributeClass{}

	queryAtttributeClassStmt := fmt.Sprintf(`SELECT %s FROM attribute_classes WHERE class_id = ?`, AttributeClassField())
	if err = ac.ScanRow(tx.QueryRow(queryAtttributeClassStmt, acid)); err != nil {
		return
	}
	switch ac.AttributeType {
	case AttributeTypeText:
		ac.Impl = &TextAttributeClass{AttributeClass: ac}
	default:
		err = fmt.Errorf("unsupport type from database : %s", ac.AttributeType)
		return
	}
	return
}

type Attribute interface {
	GetId() AttributeId                                       // 获取属性ID
	GetJSON() string                                          //获取值的JSON表示
	GetType() string                                          //获取class ID
	GetClassId() AttributeClassId                             //获取class ID
	SetValue(map[string]interface{}) error                    //设置值
	InsertData(tx *sql.Tx, objId object.ObjectId) (err error) //插入数据和索引
	UpdateData(tx *sql.Tx) (err error)                        //更新数据和索引
	DeleteData(tx *sql.Tx) (err error)                        //删除数据和索引
}

const (
	AttributeTypeText = "text"
)

type AttributeStore struct {
	ObjectId      uuid.UUID
	AttributeId   uuid.UUID
	AttributeType string
	UpdateDate    time.Time
	Data          string
}
