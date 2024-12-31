package paroket

import "testing"

func testSqliteImpl(t *testing.T) {
	// Test the implementation of the SqliteImpl struct
	var db Paroket
	db = NewSqliteImpl()
}
