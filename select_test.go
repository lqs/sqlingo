package sqlingo

import (
	"context"
	"testing"
)

type table1 struct {
	Table
}

var Table1 = table1{
	NewTable("table1"),
}
var field1 = NewNumberField("table1", "field1")
var field2 = NewNumberField("table1", "field2")

func (t table1) GetFields() []Field {
	return []Field{field1, field2}
}

func (t table1) GetFieldsSQL() string {
	return "<fields sql>"
}

func (t table1) GetFullFieldsSQL() string {
	return "<full fields sql>"
}

func TestSelect(t *testing.T) {
	db := newMockDatabase()
	assertValue(t, db.Select(1), "(SELECT 1)")

	table2 := NewTable("table1")
	field3 := NewNumberField("table2", "field1")

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
}

func TestLock(t *testing.T) {
	db := database{}
	table1 := NewTable("table1")
	db.Select(1).From(table1).LockInShareMode()
	db.Select(1).From(table1).ForUpdate()
}
