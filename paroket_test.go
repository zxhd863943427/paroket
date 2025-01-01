package paroket

import (
	"testing"

	"paroket/attribute"
	"paroket/object"
	"paroket/table"

	"github.com/stretchr/testify/assert"
)

func TestSqliteImpl(t *testing.T) {
	// 初始化测试数据库
	dbPath := "test.db"
	// defer os.Remove(dbPath)

	sqlite := NewSqliteImpl()
	err := sqlite.LoadDB(dbPath)
	assert.NoError(t, err)

	// 测试数据库初始化
	err = sqlite.InitDB()
	assert.NoError(t, err)
	defer sqlite.db.Close()

	// 测试对象操作
	t.Run("Test Object Operations", func(t *testing.T) {
		// 创建新对象
		objId, err := object.NewObjectId()
		assert.NoError(t, err)
		obj := &object.Object{ObjectId: objId}

		// 添加对象
		addedObj, err := sqlite.AddObject(obj)
		assert.NoError(t, err)
		assert.Equal(t, obj.ObjectId, addedObj.ObjectId)

		// 删除对象
		removedObj, err := sqlite.RemoveObject(obj.ObjectId)
		assert.NoError(t, err)
		assert.Equal(t, obj.ObjectId, removedObj.ObjectId)
	})

	// 测试属性类操作
	t.Run("Test Attribute Class Operations", func(t *testing.T) {
		// 创建新属性类
		ac, err := attribute.NewAttributeClass("text")
		assert.NoError(t, err)

		// 添加属性类
		addedAc, err := sqlite.AddAttributeClass(ac)
		assert.NoError(t, err)
		assert.Equal(t, ac.ClassId, addedAc.ClassId)

		// 更新属性类
		ac.AttributeName = "updated name"
		updatedAc, err := sqlite.UpdateAttributeClass(ac)
		assert.NoError(t, err)
		assert.Equal(t, ac.AttributeName, updatedAc.AttributeName)

		// 删除属性类
		removedAc, err := sqlite.RemoveAttributeClass(ac.ClassId)
		assert.NoError(t, err)
		assert.Equal(t, ac.ClassId, removedAc.ClassId)
	})

	// 测试表操作
	t.Run("Test Table Operations", func(t *testing.T) {
		// 创建新表
		table, err := table.NewTable()
		assert.NoError(t, err)
		table.TableName = "test table"

		// 添加表
		addedTable, err := sqlite.AddTable(table)
		assert.NoError(t, err)
		assert.Equal(t, table.TableId, addedTable.TableId)

		// 更新表
		table.TableName = "updated table"
		updatedTable, err := sqlite.UpdateTable(table)
		assert.NoError(t, err)
		assert.Equal(t, "test table", updatedTable.TableName)

		// 删除表
		removedTable, err := sqlite.RemoveTable(table.TableId)
		assert.NoError(t, err)
		assert.Equal(t, table.TableId, removedTable.TableId)
	})

	// 测试关联操作
	t.Run("Test Association Operations", func(t *testing.T) {
		// 创建对象和表
		objId, err := object.NewObjectId()
		assert.NoError(t, err)
		obj := &object.Object{ObjectId: objId}

		table, err := table.NewTable()
		assert.NoError(t, err)
		table.TableName = "association test"

		// 添加对象和表
		_, err = sqlite.AddObject(obj)
		assert.NoError(t, err)
		_, err = sqlite.AddTable(table)
		assert.NoError(t, err)

		// 测试对象与表关联
		err = sqlite.AddObjectToTable(table.TableId, obj.ObjectId)
		assert.NoError(t, err)

		// 测试从表删除对象
		err = sqlite.RemoveObjectFromTable(table.TableId, obj.ObjectId)
		assert.NoError(t, err)

		// 清理
		_, err = sqlite.RemoveObject(obj.ObjectId)
		assert.NoError(t, err)
		_, err = sqlite.RemoveTable(table.TableId)
		assert.NoError(t, err)
	})
}
