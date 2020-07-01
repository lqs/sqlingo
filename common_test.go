package sqlingo

import (
	"errors"
	"strings"
	"testing"
)

func TestCommon(t *testing.T) {
	db := newMockDatabase()

	dummyExp1 := expression{sql: "<dummy 1>"}
	dummyExp2 := expression{sql: "<dummy 2>"}
	errExp := expression{builder: func(scope scope) (string, error) {
		return "", errors.New("error")
	}}
	assertValue(t, &assignment{
		field: dummyExp1,
		value: dummyExp2,
	}, "<dummy 1> = <dummy 2>")
	assertError(t, &assignment{
		field: errExp,
		value: dummyExp2,
	})
	assertError(t, &assignment{
		field: dummyExp1,
		value: errExp,
	})
	assertError(t, &assignment{
		field: errExp,
		value: errExp,
	})
	assertError(t, command("COMMAND", errExp))

	sql, err := commaExpressions(scope{}, []Expression{dummyExp1, dummyExp2, dummyExp1})
	if err != nil {
		t.Error(err)
	}
	if sql != "<dummy 1>, <dummy 2>, <dummy 1>" {
		t.Error()
	}

	_, err = commaExpressions(scope{}, []Expression{dummyExp1, dummyExp2, errExp})
	if err == nil {
		t.Error("should get error")
	}

	sql, err = commaAssignments(scope{}, []assignment{
		{field: dummyExp1, value: dummyExp1},
		{field: dummyExp1, value: dummyExp2},
		{field: dummyExp2, value: dummyExp2},
	})
	if err != nil {
		t.Error(err)
	}
	if sql != "<dummy 1> = <dummy 1>, <dummy 1> = <dummy 2>, <dummy 2> = <dummy 2>" {
		t.Error()
	}
	_, err = commaAssignments(scope{}, []assignment{
		{field: dummyExp1, value: dummyExp1},
		{field: dummyExp1, value: errExp},
	})
	if err == nil {
		t.Error("should get error")
	}

	sql, err = commaOrderBys(scope{}, []OrderBy{
		orderBy{by: dummyExp1, desc: true},
		orderBy{by: dummyExp2},
	})
	if err != nil {
		t.Error(err)
	}
	if sql != "<dummy 1> DESC, <dummy 2>" {
		t.Error()
	}

	_, err = commaOrderBys(scope{}, []OrderBy{
		orderBy{by: errExp},
	})
	if err == nil {
		t.Error("should get error")
	}

	db.EnableCallerInfo(true)
	if _, err := db.Select(1).FetchFirst(); err != nil {
		t.Error(err)
	}
	if !strings.HasPrefix(sharedMockConn.lastSql, "/* ") {
		t.Error()
	}
}
