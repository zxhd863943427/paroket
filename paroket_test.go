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
		objects := make([]*object.Object, 10)
		for i := 0; i < 10; i++ {
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
		assert.Equal(t, 10, count)

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
}
