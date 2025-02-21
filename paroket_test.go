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

var testAttributeType = []common.AttributeType{
	attribute.AttributeTypeText,
}

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
		tx, err := sqlite.WriteTx(ctx)
		defer tx.Commit()
		assert.NoError(t, err)
		obj, err := sqlite.CreateObject(ctx, tx)
		assert.NoError(t, err)

		// 验证对象是否存在于数据库
		nobj, err := sqlite.OpenObject(ctx, tx, obj.ObjectId())
		assert.NoError(t, err)
		assert.Equal(t, obj.ObjectId(), nobj.ObjectId())

		// 删除对象
		err = sqlite.DeleteObject(ctx, tx, obj.ObjectId())
		assert.NoError(t, err)

		// 验证对象是否已删除
		_, err = sqlite.OpenObject(ctx, tx, obj.ObjectId())
		assert.Equal(t, sql.ErrNoRows, err)
	})

	// 测试属性类基本操作
	t.Run("Test Attribute Class Operations", func(t *testing.T) {
		cleanupDatabase()
		tx, err := sqlite.WriteTx(ctx)
		assert.NoError(t, err)
		defer tx.Commit()
		// 创建新属性类
		for _, acType := range testAttributeType {
			ac, err := sqlite.CreateAttributeClass(ctx, tx, acType)
			assert.NoError(t, err)

			// 验证属性类更新表是否创建
			metainfo, err := ac.GetMetaInfo(ctx, tx)
			assert.NoError(t, err)
			tableName, ok := metainfo["updated_table"].(string)
			assert.True(t, ok)
			assert.NoError(t, err)
			var readName string
			err = tx.QueryRow(
				"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				tableName,
			).Scan(&readName)
			assert.NoError(t, err)

			// 更新属性类
			newName := "new name"
			ac.Set(ctx, tx, utils.JSONMap{
				"name": newName,
			})
			nac, err := sqlite.OpenAttributeClass(ctx, tx, ac.ClassId())
			assert.NoError(t, err)
			assert.Equal(t, ac.Name(), nac.Name())

			// 删除属性类
			err = sqlite.DeleteAttributeClass(ctx, tx, ac.ClassId())
			assert.NoError(t, err)

			// 验证属性类表是否删除
			assert.NoError(t, err)
			err = tx.QueryRow(
				"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				tableName,
			).Scan(&readName)
			assert.Equal(t, sql.ErrNoRows, err)
		}

	})

	// 测试表操作
	t.Run("Test Table Operations", func(t *testing.T) {
		cleanupDatabase()
		tx, err := sqlite.WriteTx(ctx)
		assert.NoError(t, err)
		defer tx.Commit()
		// 创建新表
		table, err := sqlite.CreateTable(ctx, tx)
		assert.NoError(t, err)
		// 验证表数据表是否创建
		metainfo := table.MetaInfo()
		tableKey, ok := metainfo["data_table"].(string)
		assert.True(t, ok)
		var tableName string
		assert.NoError(t, err)
		err = tx.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			tableKey).Scan(&tableName)
		assert.NoError(t, err)

		// 更新表
		newName := "new name"
		err = table.Set(ctx, tx, utils.JSONMap{
			"name": newName,
		})
		assert.NoError(t, err)
		assert.Equal(t, newName, table.Name())

		// 删除表
		err = sqlite.DeleteTable(ctx, tx, table.TableId())
		assert.NoError(t, err)

		// 验证表数据表是否删除
		err = tx.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			tableKey).Scan(&tableName)
		assert.Equal(t, sql.ErrNoRows, err)
	})

	// 测试文本属性与对象联合操作
	t.Run("Test Object TextAttributeClass Operations", func(t *testing.T) {
		cleanupDatabase()
		tx, err := sqlite.WriteTx(ctx)
		assert.NoError(t, err)
		defer tx.Commit()
		// 创建新对象
		obj, err := sqlite.CreateObject(ctx, tx)
		assert.NoError(t, err)

		// 创建新属性类
		ac, err := sqlite.CreateAttributeClass(ctx, tx, attribute.AttributeTypeText)
		assert.NoError(t, err)

		// 给对象添加属性
		attr, err := ac.Insert(ctx, tx, obj.ObjectId())
		assert.NoError(t, err)
		assert.Equal(t, attr.GetClass(), ac)

		//设置属性
		newValue := "name"
		err = attr.SetValue(map[string]interface{}{"value": newValue})
		assert.NoError(t, err)
		err = ac.Update(ctx, tx, obj.ObjectId(), attr)
		assert.NoError(t, err)

		// 验证设置的属性
		nAttr, err := ac.FindId(ctx, tx, obj.ObjectId())
		assert.NoError(t, err)
		assert.Equal(t, newValue, nAttr.String())

		assert.Equal(t, nAttr.GetClass(), ac)

		// 删除属性
		err = ac.Delete(ctx, tx, obj.ObjectId())
		assert.NoError(t, err)

		// 验证是否删除
		_, err = ac.FindId(ctx, tx, obj.ObjectId())
		assert.Equal(t, sql.ErrNoRows, err)

	})

	// 测试对象与表联合操作
	t.Run("Test table Objects Constraints", func(t *testing.T) {

		cleanupDatabase()
		tx, err := sqlite.WriteTx(ctx)
		assert.NoError(t, err)
		defer tx.Commit()
		var objNum = 20
		// 创建新对象列表
		objIdList := []common.ObjectId{}
		objIdMap := map[common.ObjectId]bool{}
		for i := 0; i < objNum; i++ {
			obj, err := sqlite.CreateObject(ctx, tx)
			assert.NoError(t, err)

			objIdList = append(objIdList, obj.ObjectId())
			objIdMap[obj.ObjectId()] = true
		}

		// 创建新表
		table, err := sqlite.CreateTable(ctx, tx)
		assert.NoError(t, err)

		err = table.Insert(ctx, tx, objIdList...)
		assert.NoError(t, err)

		newObjList, err := table.FindId(ctx, tx, objIdList...)
		assert.NoError(t, err)

		newObjIdMap := map[common.ObjectId]bool{}
		for _, nobj := range newObjList {
			assert.True(t, objIdMap[nobj.ObjectId()])
			newObjIdMap[nobj.ObjectId()] = true
		}
		for objId := range objIdMap {
			assert.True(t, newObjIdMap[objId])

		}
		// 测试删除
		err = table.Delete(ctx, tx, objIdList...)
		assert.NoError(t, err)
		delObjList, err := table.FindId(ctx, tx, objIdList...)
		assert.NoError(t, err)
		assert.Equal(t, len(delObjList), 0)

	})

	// 测试属性类与表的关联操作
	t.Run("Test Attribute Class Table Operations", func(t *testing.T) {
		cleanupDatabase()
		tx, err := sqlite.WriteTx(ctx)
		assert.NoError(t, err)
		defer tx.Commit()
		var objNum = 20
		var acNum = 4
		// 创建新对象列表
		objIdList := []common.ObjectId{}
		objIdMap := map[common.ObjectId]bool{}
		for i := 0; i < objNum; i++ {
			obj, err := sqlite.CreateObject(ctx, tx)
			assert.NoError(t, err)

			objIdList = append(objIdList, obj.ObjectId())
			objIdMap[obj.ObjectId()] = true
		}

		// 创建属性类列表
		acList := []common.AttributeClass{}
		for _, acType := range testAttributeType {
			for i := 1; i < acNum; i++ {
				ac, err := sqlite.CreateAttributeClass(ctx, tx, acType)
				assert.NoError(t, err)
				acList = append(acList, ac)

			}
		}
		table, err := sqlite.CreateTable(ctx, tx)
		assert.NoError(t, err)

		//添加属性到对象
		for _, oid := range objIdList {
			for _, ac := range acList {
				_, err = ac.Insert(ctx, tx, oid)
				assert.NoError(t, err)

			}
		}
		//添加属性到表
		for _, ac := range acList {
			err = table.AddAttributeClass(ctx, tx, ac)
			assert.NoError(t, err)
		}

		//插入对象到表
		table.Insert(ctx, tx, objIdList...)

		for idx, ac := range acList {
			if idx%2 == 0 {
				continue
			}
			err = table.DeleteAttributeClass(ctx, tx, ac)
			assert.NoError(t, err)
		}
		for idx, oid := range objIdList {
			if idx%2 == 0 {
				continue
			}
			table.Delete(ctx, tx, oid)
		}
	})

}
