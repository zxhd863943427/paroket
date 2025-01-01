package paroket

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"paroket/attribute"
	"paroket/object"
	"paroket/table"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSqliteImpl(t *testing.T) {
	// 创建testdata目录
	err := os.MkdirAll("testdata", 0755)
	assert.NoError(t, err)

	// 为每个测试用例创建独立的数据库文件
	dbPath := fmt.Sprintf("testdata/test_%s.db", t.Name())

	// 初始化sqlite实例
	sqlite := NewSqliteImpl()

	// 确保数据库连接关闭并删除测试数据库
	defer func() {
		if sqlite.db != nil {
			t.Log("Closing database connection...")
			if err := sqlite.db.Close(); err != nil {
				t.Logf("Failed to close database connection: %v", err)
			} else {
				t.Log("Database connection closed successfully")
			}
		}
		if err := os.Remove(dbPath); err != nil {
			t.Logf("Failed to remove database file: %v", err)
		} else {
			t.Log("Database file removed successfully")
		}
	}()

	err = sqlite.LoadDB(dbPath)
	assert.NoError(t, err)

	// 测试数据库初始化
	err = sqlite.InitDB()
	assert.NoError(t, err)

	// 在每个测试用例前清理数据库
	cleanupDatabase := func() {
		tables := []string{
			"tables", "table_views", "objects",
			"attribute_classes", "table_to_attribute_classes",
			"object_to_attribute_classes", "object_to_tables",
		}

		for _, tableName := range tables {
			_, err := sqlite.db.Exec(fmt.Sprintf("DELETE FROM %s", tableName))
			if err != nil {
				t.Logf("Failed to clean up table %s: %v", tableName, err)
			}
		}
	}

	// 验证数据库表结构
	t.Run("Test Database Schema", func(t *testing.T) {
		cleanupDatabase()
		tables := []string{
			"tables", "table_views", "objects",
			"attribute_classes", "table_to_attribute_classes",
			"object_to_attribute_classes", "object_to_tables",
		}

		for _, tableName := range tables {
			var exists bool
			err := sqlite.db.QueryRow(
				"SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
				tableName,
			).Scan(&exists)
			assert.NoError(t, err)
			assert.True(t, exists, "Table %s should exist", tableName)
		}
	})

	// 测试对象操作
	t.Run("Test Object Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建新对象
		objId, err := object.NewObjectId()
		assert.NoError(t, err)
		obj := &object.Object{ObjectId: objId}

		// 添加对象
		addedObj, err := sqlite.AddObject(obj)
		assert.NoError(t, err)
		assert.Equal(t, obj.ObjectId, addedObj.ObjectId)

		// 验证对象是否存在于数据库
		var dbObjId uuid.UUID
		err = sqlite.db.QueryRow("SELECT object_id FROM objects WHERE object_id = ?", obj.ObjectId).Scan(&dbObjId)
		assert.NoError(t, err)
		assert.Equal(t, obj.ObjectId, object.ObjectId(dbObjId))

		// 删除对象
		removedObj, err := sqlite.RemoveObject(obj.ObjectId)
		assert.NoError(t, err)
		assert.Equal(t, obj.ObjectId, removedObj.ObjectId)

		// 验证对象是否已删除
		err = sqlite.db.QueryRow("SELECT object_id FROM objects WHERE object_id = ?", obj.ObjectId).Scan(&dbObjId)
		assert.Equal(t, sql.ErrNoRows, err)
	})

	// 测试属性类操作
	t.Run("Test Attribute Class Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建新属性类
		ac, err := attribute.NewAttributeClass("text")
		assert.NoError(t, err)

		// 添加属性类
		addedAc, err := sqlite.AddAttributeClass(ac)
		assert.NoError(t, err)
		assert.Equal(t, ac.ClassId, addedAc.ClassId)

		// 验证属性类表是否创建
		var tableName string
		err = sqlite.db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			"text_"+ac.ClassId.String(),
		).Scan(&tableName)
		assert.NoError(t, err)

		// 更新属性类
		ac.AttributeName = "updated name"
		updatedAc, err := sqlite.UpdateAttributeClass(ac)
		assert.NoError(t, err)
		assert.Equal(t, ac.AttributeName, updatedAc.AttributeName)

		// 删除属性类
		removedAc, err := sqlite.RemoveAttributeClass(ac.ClassId)
		assert.NoError(t, err)
		assert.Equal(t, ac.ClassId, removedAc.ClassId)

		// 验证属性类表是否删除
		err = sqlite.db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			"text_"+ac.ClassId.String(),
		).Scan(&tableName)
		assert.Equal(t, sql.ErrNoRows, err)
	})

	// 测试表操作
	t.Run("Test Table Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建新表
		table, err := table.NewTable()
		assert.NoError(t, err)
		table.TableName = "test table"

		// 添加表
		addedTable, err := sqlite.AddTable(table)
		assert.NoError(t, err)
		assert.Equal(t, table.TableId, addedTable.TableId)

		// 验证表数据表是否创建
		var tableName string
		err = sqlite.db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			"table_"+table.TableId.String(),
		).Scan(&tableName)
		assert.NoError(t, err)

		// 更新表
		table.TableName = "updated table"
		updatedTable, err := sqlite.UpdateTable(table)
		assert.NoError(t, err)
		assert.Equal(t, "test table", updatedTable.TableName)

		// 删除表
		removedTable, err := sqlite.RemoveTable(table.TableId)
		assert.NoError(t, err)
		assert.Equal(t, table.TableId, removedTable.TableId)

		// 验证表数据表是否删除
		err = sqlite.db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			"table_"+table.TableId.String(),
		).Scan(&tableName)
		assert.Equal(t, sql.ErrNoRows, err)
	})

	// 测试批量操作
	t.Run("Test Batch Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建多个对象
		testNum := 100
		objects := make([]*object.Object, testNum)
		for i := 0; i < testNum; i++ {
			objId, err := object.NewObjectId()
			assert.NoError(t, err)
			objects[i] = &object.Object{ObjectId: objId}
		}

		// 批量添加对象
		for _, obj := range objects {
			_, err := sqlite.AddObject(obj)
			assert.NoError(t, err)
		}

		// 验证对象数量
		var count int
		err := sqlite.db.QueryRow("SELECT COUNT(*) FROM objects").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, testNum, count)

		// 批量删除对象
		for _, obj := range objects {
			_, err := sqlite.RemoveObject(obj.ObjectId)
			assert.NoError(t, err)
		}

		// 验证对象是否全部删除
		err = sqlite.db.QueryRow("SELECT COUNT(*) FROM objects").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	// 测试外键约束
	t.Run("Test Foreign Key Constraints", func(t *testing.T) {
		var count int
		cleanupDatabase()
		// 创建对象
		objId, err := object.NewObjectId()
		assert.NoError(t, err)
		obj := &object.Object{ObjectId: objId}

		// 尝试添加对象到不存在的表
		tableId := table.TableId(uuid.New())
		err = sqlite.AddObjectToTable(tableId, obj.ObjectId)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no such table")

		// 添加对象
		_, err = sqlite.AddObject(obj)
		assert.NoError(t, err)

		// 创建表
		table, err := table.NewTable()
		assert.NoError(t, err)
		table.TableName = "foreign key test"

		// 添加表
		_, err = sqlite.AddTable(table)
		assert.NoError(t, err)

		// 添加对象到表
		err = sqlite.AddObjectToTable(table.TableId, obj.ObjectId)
		assert.NoError(t, err)

		// 删除对象，验证级联删除
		removedObj, err := sqlite.RemoveObject(obj.ObjectId)
		assert.NoError(t, err)
		assert.Equal(t, obj.ObjectId, removedObj.ObjectId)

		// 验证对象是否从关联表中删除
		err = sqlite.db.QueryRow(
			"SELECT COUNT(*) FROM object_to_tables WHERE object_id = ?",
			obj.ObjectId,
		).Scan(&count)

		if err != nil && err != sql.ErrNoRows {
			assert.NoError(t, err)
		}
		assert.Equal(t, 0, count, "Object should be removed from association table after deletion. Count: %d", count)

		// 验证对象是否从主表中删除
		err = sqlite.db.QueryRow(
			"SELECT COUNT(*) FROM objects WHERE object_id = ?",
			obj.ObjectId,
		).Scan(&count)
		if err != nil && err != sql.ErrNoRows {
			assert.NoError(t, err)
		}
		assert.Equal(t, 0, count, "Object should be removed from main table after deletion. Count: %d", count)
	})

	// 测试从表删除对象
	t.Run("Test Remove Object From Table", func(t *testing.T) {
		cleanupDatabase()
		// 创建对象
		objId, err := object.NewObjectId()
		assert.NoError(t, err)
		obj := &object.Object{ObjectId: objId}

		// 添加对象
		_, err = sqlite.AddObject(obj)
		assert.NoError(t, err)

		// 创建表
		table, err := table.NewTable()
		assert.NoError(t, err)
		table.TableName = "test table"

		// 添加表
		_, err = sqlite.AddTable(table)
		assert.NoError(t, err)

		// 添加对象到表
		err = sqlite.AddObjectToTable(table.TableId, obj.ObjectId)
		assert.NoError(t, err)

		// 从表删除对象
		err = sqlite.RemoveObjectFromTable(table.TableId, obj.ObjectId)
		assert.NoError(t, err)

		// 验证对象是否从表中删除
		var exists bool
		err = sqlite.db.QueryRow(
			"SELECT EXISTS (SELECT 1 FROM object_to_tables WHERE object_id = ? AND table_id = ?)",
			obj.ObjectId, table.TableId,
		).Scan(&exists)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	// 测试属性类与表的关联操作
	t.Run("Test Attribute Class Table Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建属性类
		ac, err := attribute.NewAttributeClass("text")
		assert.NoError(t, err)

		// 添加属性类
		_, err = sqlite.AddAttributeClass(ac)
		assert.NoError(t, err)

		// 创建表
		table, err := table.NewTable()
		assert.NoError(t, err)
		table.TableName = "test table"

		// 添加表
		_, err = sqlite.AddTable(table)
		assert.NoError(t, err)

		// 添加属性类到表
		err = sqlite.AddAttributeClassToTable(table.TableId, ac.ClassId)
		assert.NoError(t, err)

		// 验证属性类是否添加到表
		var exists bool
		err = sqlite.db.QueryRow(
			"SELECT EXISTS (SELECT 1 FROM table_to_attribute_classes WHERE table_id = ? AND class_id = ?)",
			table.TableId, ac.ClassId,
		).Scan(&exists)
		assert.NoError(t, err)
		assert.True(t, exists)

		// 从表删除属性类
		err = sqlite.RemoveAttributeClassFromTable(table.TableId, ac.ClassId)
		assert.NoError(t, err)

		// 验证属性类是否从表删除
		err = sqlite.db.QueryRow(
			"SELECT EXISTS (SELECT 1 FROM table_to_attribute_classes WHERE table_id = ? AND class_id = ?)",
			table.TableId, ac.ClassId,
		).Scan(&exists)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	// 测试对象属性操作
	t.Run("Test Object Attribute Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建对象
		objId, err := object.NewObjectId()
		assert.NoError(t, err)
		obj := &object.Object{ObjectId: objId}

		// 添加对象
		_, err = sqlite.AddObject(obj)
		assert.NoError(t, err)

		// 创建属性类
		ac, err := attribute.NewAttributeClass("text")
		assert.NoError(t, err)

		// 添加属性类
		_, err = sqlite.AddAttributeClass(ac)
		assert.NoError(t, err)

		// 添加属性到对象
		attr, err := ac.NewAttribute()
		assert.NoError(t, err)
		err = sqlite.AddAttributeToObject(obj.ObjectId, attr)
		assert.NoError(t, err)

		// 验证属性是否添加到对象
		var exists bool
		err = sqlite.db.QueryRow(
			"SELECT EXISTS (SELECT 1 FROM object_to_attribute_classes WHERE object_id = ? AND class_id = ?)",
			obj.ObjectId, ac.ClassId,
		).Scan(&exists)
		assert.NoError(t, err)
		assert.True(t, exists)

		// 从对象删除属性类
		err = sqlite.RemoveAttributeClassFromObject(obj.ObjectId, ac.ClassId)
		assert.NoError(t, err)

		// 验证属性类是否从对象删除
		err = sqlite.db.QueryRow(
			"SELECT EXISTS (SELECT 1 FROM object_to_attribute_classes WHERE object_id = ? AND class_id = ?)",
			obj.ObjectId, ac.ClassId,
		).Scan(&exists)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	// 测试列表操作
	t.Run("Test List Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建多个属性类
		ac1, err := attribute.NewAttributeClass("text")
		assert.NoError(t, err)
		// ac2, err := attribute.NewAttributeClass("number")
		ac2, err := attribute.NewAttributeClass("text")
		assert.NoError(t, err)

		// 添加属性类
		_, err = sqlite.AddAttributeClass(ac1)
		assert.NoError(t, err)
		_, err = sqlite.AddAttributeClass(ac2)
		assert.NoError(t, err)

		// 测试获取属性类列表
		acList, err := sqlite.ListAttributeClasses()
		assert.NoError(t, err)
		assert.Len(t, acList, 2)

		// 创建多个表
		table1, err := table.NewTable()
		assert.NoError(t, err)
		table1.TableName = "table1"
		table2, err := table.NewTable()
		assert.NoError(t, err)
		table2.TableName = "table2"

		// 添加表
		_, err = sqlite.AddTable(table1)
		assert.NoError(t, err)
		_, err = sqlite.AddTable(table2)
		assert.NoError(t, err)

		// 测试获取表列表
		tableList, err := sqlite.ListTables()
		assert.NoError(t, err)
		assert.Len(t, tableList, 2)

		// 创建对象
		objId, err := object.NewObjectId()
		assert.NoError(t, err)
		obj := &object.Object{ObjectId: objId}

		// 添加对象
		_, err = sqlite.AddObject(obj)
		assert.NoError(t, err)

		// 添加属性到对象
		attr, err := ac1.NewAttribute()
		assert.NoError(t, err)
		err = sqlite.AddAttributeToObject(obj.ObjectId, attr)
		assert.NoError(t, err)

		// 测试获取属性类关联的对象列表
		objList, err := sqlite.ListAttributeClassObjects(ac1.ClassId)
		assert.NoError(t, err)
		assert.Len(t, objList, 1)

		// 测试获取对象关联的属性列表
		attrList, err := sqlite.ListObjectAttributes(obj.ObjectId)
		assert.NoError(t, err)
		assert.Len(t, attrList, 1)
	})
}
