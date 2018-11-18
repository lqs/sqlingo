package sqlingo

import (
	"fmt"
	"runtime"
	"strings"
)

type Model interface {
	GetTable() Table
	GetValues() []interface{}
}

type Assignment interface {
	GetSQL() string
}

type assignment struct {
	Assignment
	field Field
	value interface{}
}

func (a *assignment) GetSQL() string {
	value, _ := getSQLFromWhatever(a.value)
	return a.field.GetSQL() + " = " + value
}

func Raw(sql string) UnknownExpression {
	return &expression{sql: sql, priority: 99}
}

func And(expressions ...BooleanExpression) (result BooleanExpression) {
	if len(expressions) == 0 {
		result = &expression{sql: "TRUE"}
		return
	}
	for i, condition := range expressions {
		if i == 0 {
			result = condition
		} else {
			result = result.And(condition)
		}
	}
	return
}

func Or(expressions ...BooleanExpression) (result BooleanExpression) {
	if len(expressions) == 0 {
		result = &expression{sql: "FALSE"}
		return
	}
	for i, condition := range expressions {
		if i == 0 {
			result = condition
		} else {
			result = result.Or(condition)
		}
	}
	return
}

func Function(name string, args ...Expression) Expression {
	return &expression{sql: name + "(" + commaExpressions(args) + ")"}
}

func If(predicate Expression, trueValue Expression, falseValue Expression) (result Expression) {
	return Function("IF", predicate, trueValue, falseValue)
}

func commaFields(fields []Field) string {
	sql := ""
	for i, item := range fields {
		if i > 0 {
			sql += ", "
		}
		sql += item.GetSQL()
	}
	return sql
}

func commaExpressions(expressions []Expression) string {
	sql := ""
	for i, item := range expressions {
		if i > 0 {
			sql += ", "
		}
		sql += item.GetSQL()
	}
	return sql
}

func commaValues(values []interface{}) string {
	sql := ""
	for i, item := range values {
		if i > 0 {
			sql += ", "
		}
		value, _ := getSQLFromWhatever(item)
		sql += value
	}
	return sql
}

func commaAssignments(assignments []assignment) string {
	sql := ""
	for i, item := range assignments {
		if i > 0 {
			sql += ", "
		}
		sql += item.GetSQL()
	}
	return sql
}

func commaOrderBys(orderBys []OrderBy) string {
	sql := ""
	for i, item := range orderBys {
		if i > 0 {
			sql += ", "
		}
		sql += item.GetSQL()
	}
	return sql
}

func getSQLForName(name string) string {
	// TODO: check reserved words
	return "`" + name + "`"
}

func getCallerInfo() string {
	for i := 0; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		segs := strings.Split(file, "/")
		name := segs[len(segs)-1]
		switch name {
		case "common.go", "select.go", "insert.go", "update.go", "delete.go":
			continue
		default:
			return fmt.Sprintf("/* %s:%d */ ", name, line)
		}
	}
	return ""
}
