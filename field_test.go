package sqlingo

import (
	"errors"
	"testing"
)

type dummyTable struct {
}

func (d dummyTable) GetName() string {
	panic("should not be here")
}

func (d dummyTable) GetSQL(scope scope) string {
	panic("should not be here")
}

func (d dummyTable) GetFieldByName(name string) Field {
	panic("should not be here")
}

func (d dummyTable) GetFields() []Field {
	panic("implement me")
}

func (d dummyTable) GetFieldsSQL() string {
	return "<fields sql>"
}

func (d dummyTable) GetFullFieldsSQL() string {
	return "<full fields sql>"
}

func TestField(t *testing.T) {
	assertValue(t, NewNumberField("t1", "f1").Equals(1), "`t1`.`f1` = 1")
	assertValue(t, NewBooleanField("t1", "f1").Equals(true), "`t1`.`f1` = 1")
	assertValue(t, NewStringField("t1", "f1").Equals("x"), "`t1`.`f1` = 'x'")

	sql, _ := FieldList{}.GetSQL(scope{
		Tables: []Table{
			&dummyTable{},
		},
	})
	if sql != "<fields sql>" {
		t.Error(sql)
	}

	sql, _ = FieldList{}.GetSQL(scope{
		Tables: []Table{
			&dummyTable{},
			&dummyTable{},
		},
	})
	if sql != "<full fields sql>, <full fields sql>" {
		t.Error(sql)
	}

	if _, err := (FieldList{
		expression{builder: func(scope scope) (string, error) {
			return "", errors.New("error")
		}},
	}.GetSQL(scope{
		Tables: []Table{
			&dummyTable{},
			&dummyTable{},
		},
	})); err == nil {
		t.Error("should get error here")
	}
}
