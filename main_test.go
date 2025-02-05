package paroket

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"paroket/attribute"
	"paroket/object"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var testAttrNum = 30
var perAttrNum = 8
var testObjNum = 500 * 100 * 100

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

func TestMain(t *testing.T) {
	ticker := newTimeTicker()
	dbPath := "./testdata/test_main.db"
	// dbPath := ":memory:"
	if err := os.Remove(dbPath); err != nil {
		t.Logf("no cache db")
	} else {
		t.Logf("remove last db")
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		fmt.Println("create db err:", err)
		return
	}
	defer db.Close()
	// 启用外键支持
	if _, err = db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return
	}
	// 设置WAL模式
	if _, err = db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return
	}
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS obj_values (
		object_id BLOB PRIMARY KEY,
    	value JSONB NOT NULL
	); 
	CREATE TABLE IF NOT EXISTS tables (
		object_id BLOB PRIMARY KEY
	); 
	`)
	if err != nil {
		fmt.Println("create table err:", err)
		return
	}
	acList := []*attribute.AttributeClass{}
	for i := 0; i < testAttrNum; i++ {
		attr, err := attribute.NewAttributeClass(attribute.AttributeTypeText)
		if err != nil {
			fmt.Printf("create ac failed %v", err)
		}
		acList = append(acList, attr)
	}
	// 创建索引
	func() {
		tx, err := db.Begin()
		if err != nil {
			return
		}
		defer tx.Commit()
		for _, ac := range acList {
			_, err := tx.Exec(fmt.Sprintf(`CREATE INDEX idx_%s ON obj_values(value->'$."%s"."value"' DESC);`, ac.ClassId.String(), ac.ClassId.String()))
			if err != nil {
				fmt.Printf("create index in %s failed:%v", ac.ClassId.String(), err)
			}
		}
	}()

	objList := []*object.Object{}
	for i := 0; i <= testObjNum; i++ {
		obj, err := object.NewObject()
		if err != nil {
			fmt.Printf("create obj failed %v", err)
			return
		}
		objList = append(objList, obj)
	}

	ticker.start("insert obj")

	// value := map[string]string{}
	func() {
		tx, err := db.Begin()
		if err != nil {
			return
		}
		defer tx.Commit()
		for idx, obj := range objList {
			value := []attribute.Attribute{}
			for j := 0; j < perAttrNum; j++ {
				idx := rand.Intn(testAttrNum)
				attr, err := acList[idx].NewAttribute()
				if err != nil {
					fmt.Println("create attr failed :", err)
					return
				}
				num := rand.Intn(testObjNum)
				attr.SetValue(map[string]interface{}{"value": fmt.Sprintf("text_%d_%d", idx, num)})
				value = append(value, attr)
			}
			valueBuffer := &bytes.Buffer{}
			valueBuffer.WriteString("{")
			i := 0
			length := len(value)
			for _, value := range value {
				valueBuffer.WriteString(fmt.Sprintf(`"%s":%s`, value.GetClassId(), value.GetJSON()))
				if i < length {
					valueBuffer.WriteString(",")
				}
			}
			valueBuffer.WriteString("}")
			insertValue := valueBuffer.String()

			_, err = tx.Exec("INSERT INTO obj_values (object_id,value) VALUES (?,jsonb(?))", obj.ObjectId, insertValue)
			if err != nil {
				fmt.Println("Insert objectValue failed :", err)
				return
			}
			if idx%(testObjNum/100) == 0 {
				ticker.log("insert obj")
				fmt.Println("insert obj ", idx)
			}
		}
	}()
	ticker.log("insert obj")

	ticker.start("insert table")
	func() {
		tx, err := db.Begin()
		if err != nil {
			return
		}
		defer tx.Commit()
		for idx, obj := range objList {
			if idx%10 == 0 {
				continue
			}
			_, err = tx.Exec("INSERT INTO tables (object_id) VALUES (?)", obj.ObjectId)
			if err != nil {
				fmt.Println("Insert table failed :", err)
				return
			}

		}
	}()
	ticker.log("insert table")

	ticker.start("query offset 52 use index")

	stmt := fmt.Sprintf(`
SELECT json(value)
	FROM obj_values INNER JOIN tables t ON obj_values.object_id = t.object_id
	WHERE 
		obj_values.value->'$."%s"."value"' LIKE '%%2%%'
		AND obj_values.value->'$."%s"."value"' LIKE '%%3%%'
	ORDER BY obj_values.value->'$."%s"."value"' NULLS LAST
	LIMIT 50 OFFSET 52
		`,
		acList[3].ClassId.String(),
		acList[4].ClassId.String(),
		acList[2].ClassId.String(),
	)

	rows, err := db.Query(stmt)
	if err != nil {
		fmt.Println("query error :", err)
		return
	}
	cnt := 0
	for rows.Next() {
		cnt++

	}
	ticker.log("query offset 52 use index")

	ticker.start("query offset 52 no index")
	stmt = fmt.Sprintf(`
SELECT json(value)
	FROM obj_values INNER JOIN tables t ON obj_values.object_id = t.object_id
	WHERE 
		obj_values.value->'$."%s"."value"' LIKE '%%2%%'
		AND obj_values.value->'$."%s"."value"' LIKE '%%3%%'
	ORDER BY obj_values.value->'$."%s"."value"'+ '' NULLS LAST
	LIMIT 50 OFFSET 52000
		`,
		acList[3].ClassId.String(),
		acList[4].ClassId.String(),
		acList[2].ClassId.String(),
	)

	rows, err = db.Query(stmt)
	if err != nil {
		fmt.Println("query error :", err)
		return
	}
	cnt = 0
	for rows.Next() {
		cnt++
	}
	ticker.log("query offset 52 no index")

	ticker.start("query offset 52000 use index")

	// 	stmt := fmt.Sprintf(`
	// SELECT value->'$."%s"."value"', value->'$."%s"."value"',value->'$."%s"."value"'
	// 	FROM obj_values INNER JOIN tables t ON obj_values.object_id = t.object_id
	// 	WHERE
	// 		obj_values.value->'$."%s"."value"' LIKE '%%2%%'
	// 		AND obj_values.value->'$."%s"."value"' LIKE '%%3%%'
	// 	ORDER BY obj_values.value->'$."%s"."value"' NULLS LAST
	// 	LIMIT 50 OFFSET 52000
	// 	`,
	// 		acList[0].ClassId.String(),
	// 		acList[1].ClassId.String(),
	// 		acList[2].ClassId.String(),
	// 		acList[3].ClassId.String(),
	// 		acList[4].ClassId.String(),
	// 		acList[2].ClassId.String(),
	// 	)
	stmt = fmt.Sprintf(`
SELECT json(value)
	FROM obj_values INNER JOIN tables t ON obj_values.object_id = t.object_id
	WHERE 
		obj_values.value->'$."%s"."value"' LIKE '%%2%%'
		AND obj_values.value->'$."%s"."value"' LIKE '%%3%%'
	ORDER BY obj_values.value->'$."%s"."value"' NULLS LAST
	LIMIT 50 OFFSET 52000
		`,
		acList[3].ClassId.String(),
		acList[4].ClassId.String(),
		acList[2].ClassId.String(),
	)

	rows, err = db.Query(stmt)
	if err != nil {
		fmt.Println("query error :", err)
		return
	}
	cnt = 0
	for rows.Next() {
		cnt++
	}
	ticker.log("query offset 52000 use index")

	ticker.start("query offset 52000 no use index")
	stmt = fmt.Sprintf(`
	SELECT json(value)
		FROM obj_values INNER JOIN tables t ON obj_values.object_id = t.object_id
		WHERE 
			obj_values.value->'$."%s"."value"' LIKE '%%2%%'
			AND obj_values.value->'$."%s"."value"' LIKE '%%3%%'
		ORDER BY obj_values.value->'$."%s"."value"' + '' NULLS LAST
		LIMIT 50 OFFSET 52000
			`,
		acList[3].ClassId.String(),
		acList[4].ClassId.String(),
		acList[2].ClassId.String(),
	)

	rows, err = db.Query(stmt)
	if err != nil {
		fmt.Println("query error :", err)
		return
	}
	cnt = 0
	for rows.Next() {
		cnt++
	}
	ticker.log("query offset 52000 no use index")

	fmt.Println(stmt)
	fmt.Println(cnt)

}

func TestQuery(t *testing.T) {

	ticker := newTimeTicker()
	dbPath := "./testdata/test_main.db" + "?_journal_mode=WAL" +
		"&_synchronous=OFF" +
		"&_mmap_size=2684354560" +
		"&_secure_delete=OFF" +
		"&_cache_size=-20480" +
		"&_page_size=32768" +
		"&_busy_timeout=7000" +
		"&_ignore_check_constraints=ON" +
		"&_temp_store=MEMORY"
	fields := []string{"0194d08b_712c_7239_822d_12870ed9dbc5", "0194d08b_712c_723a_b977_1fc3997e8962", "0194d08b_712c_7238_a38e_06e6309a2654"}
	queryNum := 50
	var totalCount int
	var stmt string

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		fmt.Println("create db err:", err)
		return
	}
	defer db.Close()
	// 启用外键支持
	if _, err = db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return
	}
	// 设置WAL模式
	if _, err = db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return
	}
	ticker.start("query count use index")

	countStmt := fmt.Sprintf(`
SELECT count(*)
	FROM obj_values INNER JOIN tables t ON obj_values.object_id = t.object_id
	WHERE 
		obj_values.value->'$."%s"."value"' LIKE '%%2%%'
		AND obj_values.value->'$."%s"."value"' LIKE '%%3%%'
	ORDER BY obj_values.value->'$."%s"."value"' NULLS LAST
		`,
		fields[0],
		fields[1],
		fields[2],
	)

	countRow := db.QueryRow(countStmt)
	if err := countRow.Scan(&totalCount); err != nil {
		fmt.Println("query count error:", err)
	}
	ticker.log("query count use index")

	fmt.Println("total count:", totalCount)

	// 循环查询
	for offset := 50; offset < totalCount; offset = 2 * offset {
		fmt.Println(" ")
		//使用索引的查询
		ticker.start(fmt.Sprintf("query %d item offset %d use index", queryNum, offset))
		stmt = fmt.Sprintf(`
SELECT json(value)
	FROM obj_values INNER JOIN tables t ON obj_values.object_id = t.object_id
	WHERE 
		obj_values.value->'$."%s"."value"' LIKE '%%2%%'
		AND obj_values.value->'$."%s"."value"' LIKE '%%3%%'
	ORDER BY obj_values.value->'$."%s"."value"' NULLS LAST
	LIMIT %d OFFSET %d
		`,
			fields[0],
			fields[1],
			fields[2],
			queryNum,
			offset,
		)

		rows, err := db.Query(stmt)
		if err != nil {
			fmt.Println("query error :", err)
			return
		}
		cnt := 0
		for rows.Next() {
			cnt++
		}
		ticker.log(fmt.Sprintf("query %d item offset %d use index", queryNum, offset))

		// 不使用索引的查询

		ticker.start(fmt.Sprintf("query %d item offset %d no index", queryNum, offset))
		stmt = fmt.Sprintf(`
SELECT json(value)
	FROM obj_values INNER JOIN tables t ON obj_values.object_id = t.object_id
	WHERE 
		obj_values.value->'$."%s"."value"' LIKE '%%2%%'
		AND obj_values.value->'$."%s"."value"' LIKE '%%3%%'
	ORDER BY obj_values.value->'$."%s"."value"' + '' NULLS LAST
	LIMIT %d OFFSET %d
		`,
			fields[0],
			fields[1],
			fields[2],
			queryNum,
			offset,
		)

		rows, err = db.Query(stmt)
		if err != nil {
			fmt.Println("query error :", err)
			return
		}
		cnt = 0
		for rows.Next() {
			cnt++
		}
		ticker.log(fmt.Sprintf("query %d item offset %d no index", queryNum, offset))

	}
	fmt.Println(stmt)

}
