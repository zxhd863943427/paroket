package paroket

import (
	"paroket/attribute"
	"paroket/object"
	"paroket/table"
)

type Paroket interface {
	// 初始化数据库
	InitDB() error
	// 加载数据库
	LoadDB(string) error

	// 添加对象
	AddObject(*object.Object) (*object.Object, error)
	// 删除对象
	RemoveObject(object.ObjectId) (*object.Object, error)

	// 添加属性类
	AddAttributeClass(*attribute.AttributeClass) (*attribute.AttributeClass, error)
	// 删除属性类
	RemoveAttributeClass(attribute.AttributeClassId) (*attribute.AttributeClass, error)
	// 更新属性类
	UpdateAttributeClass(*attribute.AttributeClass) (*attribute.AttributeClass, error)

	// 添加表
	AddTable(*table.Table) (*table.Table, error)
	// 删除表
	RemoveTable(table.TableId) (*table.Table, error)
	// 更新表
	UpdateTable(*table.Table) (*table.Table, error)

	// 添加对象到表
	AddObjectToTable(table.TableId, object.ObjectId) error
	// 从表删除对象
	RemoveObjectFromTable(table.TableId, object.ObjectId) error

	// 添加属性类到表
	AddAttributeClassToTable(table.TableId, attribute.AttributeClassId) error
	// 从表删除属性类
	RemoveAttributeClassFromTable(table.TableId, attribute.AttributeClassId) error

	// 添加属性到对象
	AddAttributeToObject(object.ObjectId, attribute.Attribute) error
	// 从对象删除属性类
	RemoveAttributeClassFromObject(object.ObjectId, attribute.AttributeClassId) error

	// 获取属性类列表
	ListAttributeClasses() ([]attribute.AttributeClass, error)
	// 获取表列表
	ListTables() ([]table.Table, error)

	// 获取属性类关联的对象列表
	ListAttributeClassObjects(attribute.AttributeClassId) ([]object.Object, error)
	// 获取对象关联的属性列表
	ListObjectAttributes(object.ObjectId) ([]attribute.AttributeStore, error)
}
