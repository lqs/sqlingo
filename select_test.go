package sqlingo

import (
	"context"
	"testing"
)

type tTable1 struct {
	Table
}

var Table1 = tTable1{
	NewTable("table1"),
}

var table1 = NewTable("table1")
var field1 = NewNumberField(table1, "field1")
var field2 = NewNumberField(table1, "field2")

var table2 = NewTable("table2")
var field3 = NewNumberField(table2, "field3")

func (t tTable1) GetFields() []Field {
	return []Field{field1, field2}
}

func (t tTable1) GetFieldsSQL() string {
	return "<fields sql>"
}

func (t tTable1) GetFullFieldsSQL() string {
	return "<full fields sql>"
}

func TestSelect(t *testing.T) {
	db := newMockDatabase()
	assertValue(t, db.Select(1), "(SELECT 1)")

	db.Select(field1).From(Table1).Where(field1.Equals(42)).Limit(10).GetSQL()

	db.Select(field1, field2, field3, Count(1).As("count")).
		From(Table1, table2).
		Where(field1.Equals(field3), field2.In(db.Select(field3).From(table2))).
		GroupBy(field2).
		Having(Raw("count").GreaterThan(1)).
		OrderBy(field1.Desc(), field2).
		Limit(10).
		Offset(20).
		LockInShareMode().
		GetSQL()

	db.SelectDistinct(field2).From(Table1).GetSQL()

	db.Select(field1, field3).From(Table1).Join(table2).On(field1.Equals(field3)).GetSQL()
	db.Select(field1, field3).From(Table1).LeftJoin(table2).On(field1.Equals(field3)).GetSQL()
	db.Select(field1, field3).From(Table1).RightJoin(table2).On(field1.Equals(field3)).GetSQL()

	db.Select(1).WithContext(context.Background())

	db.SelectFrom(Table1).GetSQL()

	db.Select([]Field{field1, field2}).From(Table1).FetchFirst()
	assertLastSql(t, "SELECT `field1`, `field2` FROM `table1`")

	db.Select([]interface{}{&field1, field2, []int{3, 4}}).From(Table1).FetchFirst()
	assertLastSql(t, "SELECT `field1`, `field2`, 3, 4 FROM `table1`")

	db.Select(field1, Table1).FetchFirst()
	assertLastSql(t, "SELECT `field1`, `field1`, `field2` FROM `table1`")
}

func TestCount(t *testing.T) {
	db := newMockDatabase()

	_, _ = db.SelectFrom(Test).Count()
	assertLastSql(t, "SELECT COUNT(1) FROM `test`")

	_, _ = db.SelectDistinct(Test.F1).From(Test).Count()
	assertLastSql(t, "SELECT COUNT(DISTINCT `f1`) FROM `test`")

	_, _ = db.Select(Test.F1).From(Test).GroupBy(Test.F2).Count()
	assertLastSql(t, "SELECT COUNT(1) FROM (SELECT 1 FROM `test` GROUP BY `f2`) AS t")

	_, _ = db.SelectDistinct(Test.F1).From(Test).GroupBy(Test.F2).Count()
	assertLastSql(t, "SELECT COUNT(1) FROM (SELECT DISTINCT `f1` FROM `test` GROUP BY `f2`) AS t")

	_, _ = db.Select(Test.F1).From(Test).Exists()
	assertLastSql(t, "SELECT EXISTS (SELECT `f1` FROM `test`)")

	_, _ = db.Select(Test.F1).From(Test).Limit(10).Count()
	assertLastSql(t, "SELECT COUNT(1) FROM (SELECT 1 FROM `test` LIMIT 10) AS t")
}

func TestSelectAutoFrom(t *testing.T) {
	db := newMockDatabase()

	_, _ = db.Select(field1, field2, 123).FetchFirst()
	assertLastSql(t, "SELECT `field1`, `field2`, 123 FROM `table1`")

	_, _ = db.Select(field1, field2, 123, field3).FetchFirst()
	assertLastSql(t, "SELECT `table1`.`field1`, `table1`.`field2`, 123, `table2`.`field3` FROM `table1`, `table2`")
}

func TestFetch(t *testing.T) {
	db := newMockDatabase()
	defer func() {
		sharedMockConn.columnCount = 7
		sharedMockConn.rowCount = 10
	}()

	sharedMockConn.columnCount = 2

	_ = db
	var f1 string
	var f2 int

	ok, err := db.Select(field1, field2).From(Table1).FetchFirst(&f1, &f2)
	if !ok || err != nil {
		t.Error()
	}

	if err := db.Select(field1, field2).From(Table1).FetchExactlyOne(&f1, &f2); err == nil {
		t.Error("should get error")
	}

	sharedMockConn.rowCount = 1
	if err := db.Select(field1, field2).From(Table1).FetchExactlyOne(&f1, &f2); err != nil {
		t.Error(err)
	}

	sharedMockConn.rowCount = 0
	if err := db.Select(field1, field2).From(Table1).FetchExactlyOne(&f1, &f2); err == nil {
		t.Error("should get error")
	}

}

func TestFetchAll(t *testing.T) {
	db := newMockDatabase()

	sharedMockConn.columnCount = 2
	defer func() {
		sharedMockConn.columnCount = 7
	}()

	// fetch all as slices
	var f1s []string
	var f2s []int
	if _, err := db.Select(field1, field2).From(Table1).FetchAll(&f1s, &f2s); err != nil {
		t.Error(err)
	}
	if len(f1s) != 10 || len(f2s) != 10 {
		t.Error(f1s, f2s)
	}

	type record struct {
		unexportedF1 string
		ExportedF1   string
		unexportedF2 int
		ExportedF2   int
	}
	var records []record
	if _, err := db.Select(field1, field2).From(Table1).FetchAll(&records); err != nil {
		t.Error(err)
	}
	if len(records) != 10 {
		t.Error(records)
	}

	// fetch all as map
	var m map[string]int
	if _, err := db.Select(field1).From(Table1).FetchAll(&m); err != nil {
		t.Error(err)
	}

	// fetch all as multiple maps is illegal
	if _, err := db.Select(field1).From(Table1).FetchAll(&m, &m); err == nil {
		t.Error("should get error here")
	}

	// fetch all as unsupported type
	var unsupported int
	if _, err := db.Select(field1).From(Table1).FetchAll(&unsupported); err == nil {
		t.Error("should get error here")
	}

	// fetch all for non-pointer
	if _, err := db.Select(field1).From(Table1).FetchAll(123); err == nil {
		t.Error("should get error here")
	}
}

func TestLock(t *testing.T) {
	db := database{}
	table1 := NewTable("table1")
	db.Select(1).From(table1).LockInShareMode()
	db.Select(1).From(table1).ForUpdate()
}

func TestUnion(t *testing.T) {
	db := newMockDatabase()
	table1 := NewTable("table1")
	table2 := NewTable("table2")

	cond1 := Raw("<condition 1>")
	cond2 := Raw("<condition 2>")

	_, _ = db.SelectFrom(table1).UnionSelectFrom(table2).Where(cond1).FetchAll()
	assertLastSql(t, "SELECT * FROM `table1` UNION SELECT * FROM `table2` WHERE <condition 1>")

	_, _ = db.SelectFrom(table1).Where(cond1).
		UnionSelectFrom(table2).Where(cond2).FetchAll()
	assertLastSql(t, "SELECT * FROM `table1` WHERE <condition 1> UNION SELECT * FROM `table2` WHERE <condition 2>")

	_, _ = db.SelectFrom(table1).Where(Raw("C1")).
		UnionSelectFrom(table2).Where(Raw("C2")).
		UnionSelect(3).From(table2).Where(Raw("C3")).
		UnionSelectDistinct(4).From(table2).Where(Raw("C4")).
		UnionAllSelectFrom(table2).Where(Raw("C5")).
		UnionAllSelect(6).From(table2).Where(Raw("C6")).
		UnionAllSelectDistinct(7).From(table2).Where(Raw("C7")).
		FetchAll()
	assertLastSql(t, "SELECT * FROM `table1` WHERE C1 "+
		"UNION SELECT * FROM `table2` WHERE C2 "+
		"UNION SELECT 3 FROM `table2` WHERE C3 "+
		"UNION SELECT DISTINCT 4 FROM `table2` WHERE C4 "+
		"UNION ALL SELECT * FROM `table2` WHERE C5 "+
		"UNION ALL SELECT 6 FROM `table2` WHERE C6 "+
		"UNION ALL SELECT DISTINCT 7 FROM `table2` WHERE C7")

	_, _ = db.SelectFrom(table1).Where(Raw("C1")).
		UnionSelectFrom(table2).Where(Raw("C2")).
		UnionSelect(3).From(table2).Where(Raw("C3")).
		UnionSelectDistinct(4).From(table2).Where(Raw("C4")).
		UnionAllSelectFrom(table2).Where(Raw("C5")).
		UnionAllSelect(6).From(table2).Where(Raw("C6")).
		UnionAllSelectDistinct(7).From(table2).Where(Raw("C7")).
		Count()
	assertLastSql(t, "SELECT COUNT(1) FROM ("+
		"SELECT 1 FROM `table1` WHERE C1 "+
		"UNION SELECT * FROM `table2` WHERE C2 "+
		"UNION SELECT 3 FROM `table2` WHERE C3 "+
		"UNION SELECT DISTINCT 4 FROM `table2` WHERE C4 "+
		"UNION ALL SELECT * FROM `table2` WHERE C5 "+
		"UNION ALL SELECT 6 FROM `table2` WHERE C6 "+
		"UNION ALL SELECT DISTINCT 7 FROM `table2` WHERE C7"+
		") AS t")
}
