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

type AttributeClass struct {
	ClassId           AttributeClassId
	AttributeName     string
	AttributeType     string
	AttributeMetaInfo utils.JSONMap
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

func getTableName(at string, cid AttributeClassId) string {
	return fmt.Sprintf(
		`%s_%s`,
		at,
		cid.String())
}

func getIndexName(at string, cid AttributeClassId) string {
	return fmt.Sprintf(
		`%s_%s_idx`,
		at,
		cid.String())
}

func (ac *AttributeClass) GetDataTableName() string {
	return getTableName(ac.AttributeType, ac.ClassId)
}

func (ac *AttributeClass) GetDataIndexName() string {
	return getIndexName(ac.AttributeType, ac.ClassId)
}

func (ac *AttributeClass) NewAttribute() (attr Attribute, err error) {
	id, err := uuid.NewV7()
	if err != nil {
		return
	}
	switch ac.AttributeType {
	case AttributeTypeText:
		attr = &TextAttribute{
			id:      AttributeId(id),
			classId: ac.ClassId,
			value:   "",
		}
	default:
		err = fmt.Errorf("un support type")
	}

	return
}

func (ac *AttributeClass) ScanRow(row *sql.Row) (err error) {
	if ac == nil {
		return fmt.Errorf(`nil of AttributeClass`)
	}
	err = row.Scan(&ac.ClassId, &ac.AttributeName, &ac.AttributeType, &ac.AttributeMetaInfo)
	return
}

func (ac *AttributeClass) ScanRows(rows *sql.Rows) (err error) {
	if ac == nil {
		return fmt.Errorf(`nil of AttributeClass`)
	}
	err = rows.Scan(&ac.ClassId, &ac.AttributeName, &ac.AttributeType, &ac.AttributeMetaInfo)
	return
}

func (ac *AttributeClass) InsertClass(tx *sql.Tx, tableName string) (err error) {
	addAttributeClassStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, AttributeClassField(), InsertField())
	_, err = tx.Exec(addAttributeClassStmt, ac.ClassId, ac.AttributeName, ac.AttributeType, ac.AttributeMetaInfo)
	return
}

func (ac *AttributeClass) CreateDataTable(tx *sql.Tx) (err error) {
	var dataStmt, indexStmt, execIdxStmt string
	switch ac.AttributeType {
	case AttributeTypeText:
		dataStmt, indexStmt, execIdxStmt = createTextTable(ac.GetDataTableName(), ac.GetDataIndexName())
	default:
		err = fmt.Errorf("un support attribute type of %s", ac.AttributeType)
	}
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
	return
}

func NewAttributeClass(attributbuteType string) (ac *AttributeClass, err error) {
	uuid, err := uuid.NewV7()
	if err != nil {
		return
	}
	ac = &AttributeClass{
		ClassId:           AttributeClassId(uuid),
		AttributeName:     "untitled",
		AttributeType:     attributbuteType,
		AttributeMetaInfo: map[string]interface{}{},
	}

	return
}

type Attribute interface {
	GetId() AttributeId                                       // 获取属性ID
	GetJSON() string                                          //获取值的JSON表示
	GetType() string                                          //获取class ID
	GetClassId() AttributeClassId                             //获取class ID
	GetDataTableName() string                                 //获取数据表的名称
	GetDataIndexName() string                                 //获取索引表的名称
	InsertData(tx *sql.Tx, objId object.ObjectId) (err error) //插入数据和索引
	UpdateData(tx *sql.Tx) (err error)                        //更新数据和索引
	SearchData(tx *sql.Tx, objId object.ObjectId) (err error) //根据ObjectId搜索属性
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
