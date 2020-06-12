package sqlingo

import "testing"

func TestInsert(t *testing.T) {
	db := newMockDatabase()

	if _, err := db.InsertInto(Table1).Fields(field1).
		Values(1).
		Values(2).
		OnDuplicateKeyUpdate().Set(field1, 10).
		Execute(); err != nil {
		t.Error(err)
	}
	assertLastSql(t, "INSERT INTO `table1` (`field1`)"+
		" VALUES (1), (2)"+
		" ON DUPLICATE KEY UPDATE `field1` = 10")
}
