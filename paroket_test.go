package paroket_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"paroket"
	"paroket/attribute"
	"paroket/common"
	"paroket/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqliteImpl(t *testing.T) {
	// 创建testdata目录
	err := os.MkdirAll("testdata", 0755)
	assert.NoError(t, err)
	ctx := context.Background()

	// 为每个测试用例创建独立的数据库文件
	dbPath := fmt.Sprintf("testdata/test_%s.db", t.Name())

	// 初始化sqlite实例
	sqlite := paroket.NewSqliteImpl()

	// 确保数据库连接关闭并删除测试数据库
	defer func() {
		sqlite.Close(ctx)

		// if err := os.Remove(dbPath); err != nil {
		// 	t.Logf("Failed to remove database file: %v", err)
		// } else {
		// 	t.Log("Database file removed successfully")
		// }
	}()
	// 测试数据库初始化
	err = sqlite.Open(ctx, dbPath, nil)
	assert.NoError(t, err)

	// 在每个测试用例前清理数据库
	cleanupDatabase := func() {
		sqlite.Close(ctx)

		if err = os.Remove(dbPath); err != nil {
			t.Logf("Failed to remove database file: %v", err)
		} else {
			t.Log("Database file removed successfully")
		}

		if err = sqlite.Open(ctx, dbPath, nil); err != nil {
			t.Logf("Failed to open database file: %v", err)
		}
	}

	// 验证数据库表结构
	t.Run("Test Database Schema", func(t *testing.T) {
		cleanupDatabase()
		tables := []string{
			"tables", "table_views", "objects",
			"attribute_classes", "table_to_attribute_classes",
			"object_to_attribute_classes",
		}
		tx, err := sqlite.ReadTx(ctx)
		defer tx.Commit()

		for _, tableName := range tables {
			var exists bool

			assert.NoError(t, err)
			err = tx.QueryRow(
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
		obj, err := sqlite.CreateObject(ctx)
		assert.NoError(t, err)

		// 验证对象是否存在于数据库
		nobj, err := sqlite.OpenObject(ctx, obj.ObjectId)
		assert.NoError(t, err)
		assert.Equal(t, obj.ObjectId, nobj.ObjectId)

		// 删除对象
		err = sqlite.DeleteObject(ctx, obj.ObjectId)
		assert.NoError(t, err)

		// 验证对象是否已删除
		_, err = sqlite.OpenObject(ctx, obj.ObjectId)
		assert.Equal(t, sql.ErrNoRows, err)
	})

	// 测试属性类操作
	t.Run("Test Attribute Class Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建新属性类
		ac, err := sqlite.CreateAttributeClass(ctx, attribute.AttributeTypeText)
		assert.NoError(t, err)

		// 验证属性类更新表是否创建
		metainfo, err := ac.GetMetaInfo(ctx)
		assert.NoError(t, err)
		tableName, ok := metainfo["updated_table"].(string)
		assert.True(t, ok)
		func() {
			tx, err := sqlite.ReadTx(ctx)
			defer tx.Commit()
			assert.NoError(t, err)
			var readName string
			err = tx.QueryRow(
				"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				tableName,
			).Scan(&readName)
			assert.NoError(t, err)
		}()

		// 更新属性类
		newName := "new name"
		ac.Set(ctx, utils.JSONMap{
			"name": newName,
		})
		nac, err := sqlite.OpenAttributeClass(ctx, ac.ClassId())
		assert.NoError(t, err)
		assert.Equal(t, ac.Name(), nac.Name())

		// 删除属性类
		err = sqlite.DeleteAttributeClass(ctx, ac.ClassId())
		assert.NoError(t, err)

		// 验证属性类表是否删除
		func() {
			tx, err := sqlite.ReadTx(ctx)
			defer tx.Commit()
			assert.NoError(t, err)
			var readName string
			err = tx.QueryRow(
				"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				tableName,
			).Scan(&readName)
			assert.Equal(t, sql.ErrNoRows, err)
		}()
	})

	// 测试表操作
	t.Run("Test Table Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建新表
		table, err := sqlite.CreateTable(ctx)
		assert.NoError(t, err)
		// 验证表数据表是否创建
		metainfo := table.MetaInfo()
		tableKey, ok := metainfo["data_table"].(string)
		assert.True(t, ok)
		func() {
			var tableName string
			tx, err := sqlite.ReadTx(ctx)
			assert.NoError(t, err)
			defer tx.Commit()
			err = tx.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				tableKey).Scan(&tableName)
			assert.NoError(t, err)
		}()

		// 更新表
		newName := "new name"
		err = table.Set(ctx, utils.JSONMap{
			"name": newName,
		})
		assert.NoError(t, err)
		assert.Equal(t, newName, table.Name())

		// 删除表
		err = sqlite.DeleteTable(ctx, table.TableId())
		assert.NoError(t, err)

		// 验证表数据表是否删除
		func() {
			var tableName string
			tx, err := sqlite.ReadTx(ctx)
			assert.NoError(t, err)
			defer tx.Commit()
			err = tx.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				tableKey).Scan(&tableName)
			assert.Equal(t, sql.ErrNoRows, err)
		}()
	})

	// 测试文本属性与对象联合操作
	t.Run("Test Object TextAttributeClass Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建新对象
		obj, err := sqlite.CreateObject(ctx)
		assert.NoError(t, err)

		// 创建新属性类
		ac, err := sqlite.CreateAttributeClass(ctx, attribute.AttributeTypeText)
		assert.NoError(t, err)

		// 给对象添加属性
		attr, err := ac.Insert(ctx, obj.ObjectId)
		assert.NoError(t, err)
		assert.Equal(t, attr.GetClass(), ac)

		//设置属性
		newValue := "name"
		err = attr.SetValue(map[string]interface{}{"value": newValue})
		assert.NoError(t, err)
		err = ac.Update(ctx, obj.ObjectId, attr)
		assert.NoError(t, err)

		// 验证设置的属性
		nAttr, err := ac.FindId(ctx, obj.ObjectId)
		assert.NoError(t, err)
		assert.Equal(t, newValue, nAttr.String())

		assert.Equal(t, nAttr.GetClass(), ac)

		// 删除属性
		err = ac.Delete(ctx, obj.ObjectId)
		assert.NoError(t, err)

		// 验证是否删除
		_, err = ac.FindId(ctx, obj.ObjectId)
		assert.Equal(t, sql.ErrNoRows, err)

	})

	// 测试对象与表联合操作
	t.Run("Test table Objects Constraints", func(t *testing.T) {

		cleanupDatabase()
		var objNum = 20
		// 创建新对象列表
		objIdList := []common.ObjectId{}
		objIdMap := map[common.ObjectId]bool{}
		for i := 0; i < objNum; i++ {
			obj, err := sqlite.CreateObject(ctx)
			assert.NoError(t, err)

			objIdList = append(objIdList, obj.ObjectId)
			objIdMap[obj.ObjectId] = true
		}

		// 创建新表
		table, err := sqlite.CreateTable(ctx)
		assert.NoError(t, err)

		err = table.Insert(ctx, objIdList...)
		assert.NoError(t, err)

		newObjList, err := table.FindId(ctx, objIdList...)
		assert.NoError(t, err)

		newObjIdMap := map[common.ObjectId]bool{}
		for _, nobj := range newObjList {
			assert.True(t, objIdMap[nobj.ObjectId])
			newObjIdMap[nobj.ObjectId] = true
		}
		for objId := range objIdMap {
			assert.True(t, newObjIdMap[objId])

		}

	})

	// 测试从表删除对象
	t.Run("Test Remove Object From Table", func(t *testing.T) {
		cleanupDatabase()
		// 创建对象
		// obj, err := object.NewObject()
		// assert.NoError(t, err)

		// // 添加对象
		// _, err = sqlite.AddObject(obj)
		// assert.NoError(t, err)

		// // 创建表
		// table, err := table.NewTable()
		// assert.NoError(t, err)
		// table.TableName = "test table"

		// // 添加表
		// _, err = sqlite.AddTable(table)
		// assert.NoError(t, err)

		// // 添加对象到表
		// err = sqlite.AddObjectToTable(table.TableId, obj.ObjectId)
		// assert.NoError(t, err)

		// // 从表删除对象
		// err = sqlite.RemoveObjectFromTable(table.TableId, obj.ObjectId)
		// assert.NoError(t, err)

		// // 验证对象是否从表中删除
		// var exists bool
		// err = sqlite.db.QueryRow(
		// 	"SELECT EXISTS (SELECT 1 FROM object_to_tables WHERE object_id = ? AND table_id = ?)",
		// 	obj.ObjectId, table.TableId,
		// ).Scan(&exists)
		// assert.NoError(t, err)
		// assert.False(t, exists)
	})

	// 测试属性类与表的关联操作
	t.Run("Test Attribute Class Table Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建属性类
		// ac, err := attribute.NewAttributeClass("text")
		// assert.NoError(t, err)

		// // 添加属性类
		// _, err = sqlite.AddAttributeClass(ac)
		// assert.NoError(t, err)

		// // 创建表
		// table, err := table.NewTable()
		// assert.NoError(t, err)
		// table.TableName = "test table"

		// // 添加表
		// _, err = sqlite.AddTable(table)
		// assert.NoError(t, err)

		// // 添加属性类到表
		// err = sqlite.AddAttributeClassToTable(table.TableId, ac.ClassId)
		// assert.NoError(t, err)

		// // 验证属性类是否添加到表
		// var exists bool
		// err = sqlite.db.QueryRow(
		// 	"SELECT EXISTS (SELECT 1 FROM table_to_attribute_classes WHERE table_id = ? AND class_id = ?)",
		// 	table.TableId, ac.ClassId,
		// ).Scan(&exists)
		// assert.NoError(t, err)
		// assert.True(t, exists)

		// // 从表删除属性类
		// err = sqlite.RemoveAttributeClassFromTable(table.TableId, ac.ClassId)
		// assert.NoError(t, err)

		// // 验证属性类是否从表删除
		// err = sqlite.db.QueryRow(
		// 	"SELECT EXISTS (SELECT 1 FROM table_to_attribute_classes WHERE table_id = ? AND class_id = ?)",
		// 	table.TableId, ac.ClassId,
		// ).Scan(&exists)
		// assert.NoError(t, err)
		// assert.False(t, exists)
	})

	// 测试对象属性操作
	t.Run("Test Object Attribute Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建对象
		// obj, err := object.NewObject()
		// assert.NoError(t, err)

		// // 添加对象
		// _, err = sqlite.AddObject(obj)
		// assert.NoError(t, err)

		// // 创建属性类
		// ac, err := attribute.NewAttributeClass("text")
		// assert.NoError(t, err)

		// // 添加属性类
		// _, err = sqlite.AddAttributeClass(ac)
		// assert.NoError(t, err)

		// // 添加属性到对象
		// attr, err := ac.NewAttribute()
		// assert.NoError(t, err)
		// err = sqlite.AddAttributeToObject(obj.ObjectId, attr)
		// assert.NoError(t, err)

		// // 验证属性是否添加到对象
		// var exists bool
		// err = sqlite.db.QueryRow(
		// 	"SELECT EXISTS (SELECT 1 FROM object_to_attribute_classes WHERE object_id = ? AND class_id = ?)",
		// 	obj.ObjectId, ac.ClassId,
		// ).Scan(&exists)
		// assert.NoError(t, err)
		// assert.True(t, exists)

		// // 从对象删除属性类
		// err = sqlite.RemoveAttributeClassFromObject(obj.ObjectId, ac.ClassId)
		// assert.NoError(t, err)

		// // 验证属性类是否从对象删除
		// err = sqlite.db.QueryRow(
		// 	"SELECT EXISTS (SELECT 1 FROM object_to_attribute_classes WHERE object_id = ? AND class_id = ?)",
		// 	obj.ObjectId, ac.ClassId,
		// ).Scan(&exists)
		// assert.NoError(t, err)
		// assert.False(t, exists)
	})

	// 测试TextAttribute的SetValue方法
	t.Run("Test TextAttribute SetValue", func(t *testing.T) {
		cleanupDatabase()

		// 创建对象
		// obj, err := object.NewObject()
		// assert.NoError(t, err)

		// // 添加对象
		// _, err = sqlite.AddObject(obj)
		// assert.NoError(t, err)

		// // 创建多个Text属性类
		// testValues := []struct {
		// 	className string
		// 	value     string
		// }{
		// 	{"text1", "value1"},
		// 	{"text2", "value2"},
		// 	{"text3", "value3"},
		// }

		// for _, test := range testValues {
		// 	// 创建属性类
		// 	ac, err := attribute.NewAttributeClass(attribute.AttributeTypeText)
		// 	ac.AttributeName = test.className
		// 	assert.NoError(t, err)

		// 	// 添加属性类
		// 	_, err = sqlite.AddAttributeClass(ac)
		// 	assert.NoError(t, err)

		// 	// 创建Text属性
		// 	attr, err := ac.NewAttribute()
		// 	assert.NoError(t, err)
		// 	textAttr := attr.(*attribute.TextAttribute)

		// 	// 设置值
		// 	err = textAttr.SetValue(map[string]interface{}{"value": test.value})
		// 	assert.NoError(t, err)

		// 	// 添加属性到对象
		// 	err = sqlite.AddAttributeToObject(obj.ObjectId, textAttr)
		// 	assert.NoError(t, err)

		// 	// 验证设置的值
		// 	expectedJSON := fmt.Sprintf(`{"type": "%s", "value": "%s"}`, attribute.AttributeTypeText, test.value)
		// 	assert.Equal(t, expectedJSON, textAttr.GetJSON())
		// }

		// // 使用ListObjectAttributes获取所有属性并验证
		// attrStoreList, err := sqlite.ListObjectAttributes(obj.ObjectId)
		// assert.NoError(t, err)
		// assert.Len(t, attrStoreList, len(testValues))

		// for i, attrStore := range attrStoreList {
		// 	// textAttr := attrStore.(*attribute.TextAttribute)
		// 	expectedJSON := fmt.Sprintf(`{"type": "%s", "value": "%s"}`, attribute.AttributeTypeText, testValues[i].value)
		// 	assert.Equal(t, expectedJSON, attrStore.Data)
		// }
	})

	// 测试列表操作
	t.Run("Test List Operations", func(t *testing.T) {
		cleanupDatabase()
		// 创建多个属性类
		// ac1, err := attribute.NewAttributeClass("text")
		// assert.NoError(t, err)
		// // ac2, err := attribute.NewAttributeClass("number")
		// ac2, err := attribute.NewAttributeClass("text")
		// assert.NoError(t, err)

		// // 添加属性类
		// _, err = sqlite.AddAttributeClass(ac1)
		// assert.NoError(t, err)
		// _, err = sqlite.AddAttributeClass(ac2)
		// assert.NoError(t, err)

		// // 测试获取属性类列表
		// acList, err := sqlite.ListAttributeClasses()
		// assert.NoError(t, err)
		// assert.Len(t, acList, 2)

		// // 创建多个表
		// table1, err := table.NewTable()
		// assert.NoError(t, err)
		// table1.TableName = "table1"
		// table2, err := table.NewTable()
		// assert.NoError(t, err)
		// table2.TableName = "table2"

		// // 添加表
		// _, err = sqlite.AddTable(table1)
		// assert.NoError(t, err)
		// _, err = sqlite.AddTable(table2)
		// assert.NoError(t, err)

		// // 测试获取表列表
		// tableList, err := sqlite.ListTables()
		// assert.NoError(t, err)
		// assert.Len(t, tableList, 2)

		// // 创建对象
		// obj, err := object.NewObject()
		// assert.NoError(t, err)

		// // 添加对象
		// _, err = sqlite.AddObject(obj)
		// assert.NoError(t, err)

		// // 添加属性到对象
		// attr, err := ac1.NewAttribute()
		// assert.NoError(t, err)
		// err = sqlite.AddAttributeToObject(obj.ObjectId, attr)
		// assert.NoError(t, err)

		// // 测试获取属性类关联的对象列表
		// objList, err := sqlite.ListAttributeClassObjects(ac1.ClassId)
		// assert.NoError(t, err)
		// assert.Len(t, objList, 1)

		// // 测试获取对象关联的属性列表
		// attrList, err := sqlite.ListObjectAttributes(obj.ObjectId)
		// assert.NoError(t, err)
		// assert.Len(t, attrList, 1)
	})
}
