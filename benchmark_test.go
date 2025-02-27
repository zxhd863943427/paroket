package paroket_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"paroket"
	"paroket/attribute"
	"paroket/common"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testAttrNum = 30
var perAttrNum = 8
var testObjNum = 100 * 100
var testTableNum = 2

var logSapce = 10

type timeTicker struct {
	totalTicker map[string]time.Time
}

func newTimeTicker() *timeTicker {
	return &timeTicker{
		totalTicker: map[string]time.Time{},
	}
}

func (tt *timeTicker) start(task string) {
	tt.totalTicker[task] = time.Now()
}
func (tt *timeTicker) log(task string) {
	since := time.Since(tt.totalTicker[task])
	fmt.Printf("task %s use time :%s\n", task, since)
}

var ctx = context.Background()

func TestBenchmark(t *testing.T) {
	ticker := newTimeTicker()
	dbPath := "./testdata/test_benchmark.db"
	// dbPath := ":memory:"
	if err := os.Remove(dbPath); err != nil {
		t.Logf("no cache db")
	} else {
		t.Logf("remove last db")
	}
	db := paroket.NewSqliteImpl()
	err := db.Open(ctx, dbPath, nil)
	defer db.Close(ctx)
	if err != nil {
		fmt.Println("create db err:", err)
		return
	}
	tx, err := db.WriteTx(ctx)
	assert.NoError(t, err)
	defer tx.Commit()

	//创建对象
	ticker.start(fmt.Sprintf("create %d obj", testObjNum))
	ticker.start("insert obj")
	objList := []common.Object{}
	for i := 0; i < testObjNum; i++ {
		obj, err := db.CreateObject(ctx, tx)
		assert.NoError(t, err)
		objList = append(objList, obj)
		if i%(testObjNum/logSapce) == 0 {
			ticker.log("insert obj")
			fmt.Println("insert obj ", i)
		}
	}
	ticker.log(fmt.Sprintf("create %d obj", testObjNum))

	//创建表格
	tableList := []common.Table{}
	for i := 0; i < testTableNum; i++ {
		table, err := db.CreateTable(ctx, tx)
		assert.NoError(t, err)
		tableList = append(tableList, table)
	}
	//创建文本属性类
	acList := []common.AttributeClass{}
	for i := 0; i < testAttrNum; i++ {
		ac, err := db.CreateAttributeClass(ctx, tx, attribute.AttributeTypeText)
		assert.NoError(t, err)
		acList = append(acList, ac)
	}
	//插入属性到表格
	acStep := (testAttrNum - perAttrNum) / testTableNum
	for idx, ac := range acList {
		for jdx, table := range tableList {
			if (jdx%testAttrNum)*acStep <= idx && idx <= (jdx%testAttrNum)*acStep+perAttrNum {
				table.AddAttributeClass(ctx, tx, ac)
			}
		}
	}

	//插入对象到表格
	ticker.start("insert obj to table")
	for idx, obj := range objList {
		for jdx, table := range tableList {
			if idx%testTableNum == jdx {
				err = table.Insert(ctx, tx, obj.ObjectId())
				assert.NoError(t, err)
			}
		}
		if idx%(testObjNum/logSapce) == 0 {
			ticker.log("insert obj to table")
			fmt.Println("insert  to table ", idx)
		}
	}

	//插入属性到对象
	ticker.start("insert attribute to object")
	for idx, obj := range objList {
		for jdx, ac := range acList {
			if (idx%testAttrNum) <= jdx && jdx <= (idx%testAttrNum)+perAttrNum {
				attr, err := ac.Insert(ctx, tx, obj.ObjectId())
				assert.NoError(t, err)

				newValue := fmt.Sprintf("test_%d_%d", jdx, rand.Int31n(int32(testObjNum)))
				attr.SetValue(map[string]interface{}{"value": newValue})
				ac.Update(ctx, tx, obj.ObjectId(), attr)
			}
		}
		if idx%(testObjNum/logSapce) == 0 {
			ticker.log("insert attribute to object")
			fmt.Println("insert attribute to object ", idx)
		}
	}

}

func TestQueryBenchmark(t *testing.T) {
	ticker := newTimeTicker()
	ticker.start("query item")
	dbPath := "./testdata/test_benchmark.db"
	// dbPath := ":memory:"

	db := paroket.NewSqliteImpl()
	err := db.Open(ctx, dbPath, nil)
	defer db.Close(ctx)
	if err != nil {
		fmt.Println("create db err:", err)
		return
	}
	tx, err := db.ReadTx(ctx)
	assert.NoError(t, err)
	defer tx.Commit()

	var tid common.TableId
	err = tx.QueryRow("SELECT table_id FROM tables").Scan(&tid)
	assert.NoError(t, err)

	acList := []common.AttributeClass{}
	rows, err := tx.Query("SELECT class_id FROM table_to_attribute_classes where table_id = ?", tid)
	assert.NoError(t, err)
	for rows.Next() {
		var acid common.AttributeClassId
		err = rows.Scan(&acid)
		assert.NoError(t, err)
		ac, err := db.OpenAttributeClass(ctx, tx, acid)
		assert.NoError(t, err)
		acList = append(acList, ac)
	}
	queryBuffer := &bytes.Buffer{}
	queryBuffer.WriteString(fmt.Sprintf(`
	SELECT object_id,json(data) FROM %s WHERE`, tid.DataTable()))
	acLen := len(acList)
	for idx, ac := range acList {
		if idx > acLen/2 || idx > 2 {
			break
		}
		if idx != 0 {
			queryBuffer.WriteString(" AND ")
		}
		metainfo, err := ac.GetMetaInfo(ctx, tx)
		assert.NoError(t, err)

		valuePath, ok := metainfo["json_value_path"]
		assert.True(t, ok)
		subQuery := fmt.Sprintf(" data ->> '%s' LIKE '%%%d%%' ", valuePath, idx+1)
		queryBuffer.WriteString(subQuery)

	}
	lastAc := acList[acLen-1]
	metainfo, err := lastAc.GetMetaInfo(ctx, tx)
	assert.NoError(t, err)
	valuePath, ok := metainfo["json_value_path"]
	assert.True(t, ok)
	queryBuffer.WriteString(fmt.Sprintf(" ORDER BY data ->> '%s'", valuePath))

	queryStmt := queryBuffer.String()
	fmt.Println(queryStmt)
	rows, err = tx.Query(queryStmt)
	assert.NoError(t, err)

	objList, err := common.QueryTableObject(ctx, rows)

	ticker.log("query item")
	assert.NoError(t, err)

	fmt.Println("query item num:", len(objList))
	if len(objList) < 50 {
		for _, obj := range objList {
			fmt.Println(obj.ObjectId(), string(obj.Data()))
		}
	}

}
