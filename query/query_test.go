package query_test

import (
	"database/sql"
	"fmt"
	"os"
	"paroket"
	"testing"

	"paroket/attribute"
	"paroket/object"
	"paroket/table"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	// 创建testdata目录
	err := os.MkdirAll("testdata", 0755)
	assert.NoError(t, err)

	// 为每个测试用例创建独立的数据库文件
	dbPath := fmt.Sprintf("testdata/test_%s.db", t.Name())

	// 初始化sqlite实例
	sqlite := paroket.NewSqliteImpl()

	removeDB := func(sqlite paroket.Paroket, dbPath string) {
		if err := sqlite.Close(); err != nil {
			t.Logf("Failed to close database connection: %v", err)
		} else {
			t.Log("Database connection closed successfully")
		}
		if err := os.Remove(dbPath); err != nil {
			t.Logf("Failed to remove database file: %v", err)
		} else {
			t.Log("Database file removed successfully")
		}
	}

	initDB := func(sqlite paroket.Paroket, dbPath string) {
		err = sqlite.LoadDB(dbPath)
		assert.NoError(t, err)

		// 测试数据库初始化
		err = sqlite.InitDB()
		assert.NoError(t, err)
	}

	initDB(sqlite, dbPath)

	// 确保数据库连接关闭并删除测试数据库
	// defer removeDB(sqlite, dbPath)
	defer sqlite.Close()

	// 在每个测试用例前清理数据库
	cleanupDatabase := func() {
		removeDB(sqlite, dbPath)
		initDB(sqlite, dbPath)
	}

	defer func() {
		if t.Failed() {
			db := ((*paroket.SqliteImpl)(sqlite)).GetDB()
			tables, err := getTableNames(db)
			if err != nil {
				fmt.Println(err)
				return
			}
			for _, table := range tables {
				if err := printTableData(db, table); err != nil {
					fmt.Println(err)
				}
				fmt.Println() // 分隔不同表的输出
			}
		}
	}()

	// 测试TextAttribute的SetValue方法
	t.Run("Test TextAttribute SetValue", func(t *testing.T) {
		cleanupDatabase()

		// 创建对象
		objId, err := object.NewObjectId()
		assert.NoError(t, err)
		obj := &object.Object{ObjectId: objId}

		// 添加对象
		_, err = sqlite.AddObject(obj)
		assert.NoError(t, err)

		// 创建表
		ta, err := table.NewTable()
		assert.NoError(t, err)
		_, err = sqlite.AddTable(ta)
		assert.NoError(t, err)

		//添加对象到表中
		sqlite.AddObjectToTable(ta.TableId, obj.ObjectId)

		// 创建多个Text属性类
		testValues := []struct {
			className string
			value     string
		}{
			{"text1", "value1"},
			{"text2", "value2"},
			{"text3", "value3"},
		}

		for _, test := range testValues {
			// 创建属性类
			ac, err := attribute.NewAttributeClass(attribute.AttributeTypeText)
			ac.AttributeName = test.className
			assert.NoError(t, err)

			// 添加属性类
			_, err = sqlite.AddAttributeClass(ac)
			assert.NoError(t, err)

			// 添加属性类到表
			sqlite.AddAttributeClassToTable(ta.TableId, ac.ClassId)

			// 创建Text属性
			attr, err := ac.NewAttribute()
			assert.NoError(t, err)
			textAttr := attr.(*attribute.TextAttribute)

			// 设置值
			err = textAttr.SetValue(map[string]interface{}{"value": test.value})
			assert.NoError(t, err)

			// 添加属性到对象
			err = sqlite.AddAttributeToObject(obj.ObjectId, textAttr)
			assert.NoError(t, err)

			// 验证设置的值
			expectedJSON := fmt.Sprintf(`{"type": "%s", "value": "%s"}`, attribute.AttributeTypeText, test.value)
			assert.Equal(t, expectedJSON, textAttr.GetJSON())
		}

		// 使用query查询属性
		q, err := sqlite.GetQuery(ta.TableId)
		assert.NoError(t, err)
		data, err := sqlite.Query(q)
		assert.NoError(t, err)
		fmt.Println(data)
	})

}

func getTableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

func printTableData(db *sql.DB, tableName string) error {
	// 查询表的所有数据
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		return err
	}
	defer rows.Close()

	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// 打印表名
	fmt.Printf("Table: %s\n", tableName)

	// 打印列名
	for _, col := range columns {
		fmt.Printf("%s\t", col)
	}
	fmt.Println()

	// 打印数据
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}
		for _, val := range values {
			fmt.Printf("%v\t", val)
		}
		fmt.Println()
	}
	return nil
}
