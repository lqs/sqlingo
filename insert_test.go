package sqlingo

import (
	"errors"
	"testing"
)

type tTest struct {
	Table

	F1 fTestF1
	F2 fTestF2
}

func (t tTest) GetFields() []Field {
	return []Field{t.F1, t.F2}
}

type fTestF1 struct{ NumberField }
type fTestF2 struct{ StringField }

type TestModel struct {
	F1 int64
	F2 string
}

var tTestTable = NewTable("test")

var Test = tTest{
	Table: NewTable("test"),
	F1:    fTestF1{NewNumberField(tTestTable, "f1")},
	F2:    fTestF2{NewStringField(tTestTable, "f2")},
}

func (m TestModel) GetTable() Table {
	return Test
}

func (m TestModel) GetValues() []interface{} {
	return []interface{}{m.F1, m.F2}
}

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

	if _, err := db.InsertInto(Table1).Fields(field1).
		Values(1).
		Values(2).
		OnDuplicateKeyIgnore().
		Execute(); err != nil {
		t.Error(err)
	}
	assertLastSql(t, "INSERT INTO `table1` (`field1`)"+
		" VALUES (1), (2)"+
		" ON DUPLICATE KEY UPDATE `field1` = `field1`")

	model := &TestModel{
		F1: 1,
		F2: "test",
	}
	if _, err := db.InsertInto(Test).Values(1, 2).Execute(); err != nil {
		t.Error(err)
	}
	assertLastSql(t, "INSERT INTO `test` (`f1`, `f2`) VALUES (1, 2)")

	if _, err := db.InsertInto(Test).Models(model, &model, []Model{model}).Execute(); err != nil {
		t.Error(err)
	}
	assertLastSql(t, "INSERT INTO `test` (`f1`, `f2`) VALUES (1, 'test'), (1, 'test'), (1, 'test')")

	if _, err := db.InsertInto(Test).Models(model, &model, []interface{}{model, "invalid type"}).Execute(); err == nil {
		t.Error("should get error here")
	}

	if _, err := db.InsertInto(Table1).Models(model).Execute(); err == nil {
		t.Error("should get error here")
	}

	if _, err := db.ReplaceInto(Test).Values(1, 2).Execute(); err != nil {
		t.Error(err)
	}
	assertLastSql(t, "REPLACE INTO `test` (`f1`, `f2`) VALUES (1, 2)")

	errExpr := expression{
		builder: func(scope scope) (string, error) {
			return "", errors.New("error")
		},
	}
	if _, err := db.InsertInto(Test).Fields(errExpr).Values(1).Execute(); err == nil {
		t.Error("should get error here")
	}
	if _, err := db.InsertInto(Test).Fields(Test.F1).Values(errExpr).Execute(); err == nil {
		t.Error("should get error here")
	}
	if _, err := db.InsertInto(Test).
		Fields(Test.F1).Values(1).
		OnDuplicateKeyUpdate().Set(Test.F1, errExpr).Execute(); err == nil {
		t.Error("should get error here")
	}
}
