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
	GetSQL(scope scope) (string, error)
}

type assignment struct {
	Assignment
	field Field
	value interface{}
}

func (a assignment) GetSQL(scope scope) (string, error) {
	value, _, err := getSQLFromWhatever(scope, a.value)
	if err != nil {
		return "", err
	}
	fieldSql, err := a.field.GetSQL(scope)
	if err != nil {
		return "", err
	}
	return fieldSql + " = " + value, nil
}

func Raw(sql string) UnknownExpression {
	return expression{
		sql:      sql,
		priority: 99,
	}
}

func And(expressions ...BooleanExpression) (result BooleanExpression) {
	if len(expressions) == 0 {
		result = trueExpression()
		return
	}
	for _, condition := range expressions {
		if result == nil {
			result = condition
		} else {
			result = result.And(condition)
		}
	}
	return
}

func Or(expressions ...BooleanExpression) (result BooleanExpression) {
	if len(expressions) == 0 {
		result = falseExpression()
		return
	}
	for _, condition := range expressions {
		if result == nil {
			result = condition
		} else {
			result = result.Or(condition)
		}
	}
	return
}

func command(args ...interface{}) expression {
	return expression{builder: func(scope scope) (string, error) {
		sql := ""
		for i, item := range args {
			if i > 0 {
				sql += " "
			}
			itemSql, _, err := getSQLFromWhatever(scope, item)
			if err != nil {
				return "", err
			}
			sql += itemSql

		}
		return sql, nil
	}}
}

func commaFields(scope scope, fields []Field) (string, error) {
	var sqlBuilder strings.Builder
	for i, item := range fields {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		itemSql, err := item.GetSQL(scope)
		if err != nil {
			return "", err
		}
		sqlBuilder.WriteString(itemSql)
	}
	return sqlBuilder.String(), nil
}

func commaExpressions(scope scope, expressions []Expression) (string, error) {
	var sqlBuilder strings.Builder
	for i, item := range expressions {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		itemSql, err := item.GetSQL(scope)
		if err != nil {
			return "", err
		}
		sqlBuilder.WriteString(itemSql)
	}
	return sqlBuilder.String(), nil
}

func commaTables(scope scope, tables []Table) string {
	var sqlBuilder strings.Builder
	sqlBuilder.Grow(32)
	for i, table := range tables {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		sqlBuilder.WriteString(table.GetSQL(scope))
	}
	return sqlBuilder.String()
}

func commaValues(scope scope, values []interface{}) (string, error) {
	var sqlBuilder strings.Builder
	for i, item := range values {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		itemSql, _, err := getSQLFromWhatever(scope, item)
		if err != nil {
			return "", err
		}
		sqlBuilder.WriteString(itemSql)
	}
	return sqlBuilder.String(), nil
}

func commaAssignments(scope scope, assignments []assignment) (string, error) {
	var sqlBuilder strings.Builder
	for i, item := range assignments {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		itemSql, err := item.GetSQL(scope)
		if err != nil {
			return "", err
		}
		sqlBuilder.WriteString(itemSql)
	}
	return sqlBuilder.String(), nil
}

func commaOrderBys(scope scope, orderBys []OrderBy) (string, error) {
	var sqlBuilder strings.Builder
	for i, item := range orderBys {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		itemSql, err := item.GetSQL(scope)
		if err != nil {
			return "", err
		}
		sqlBuilder.WriteString(itemSql)
	}
	return sqlBuilder.String(), nil
}

func getSQLForName(name string) string {
	// TODO: check reserved words
	return "`" + name + "`"
}

func getCallerInfo(db database, retry bool) string {
	if !db.enableCallerInfo {
		return ""
	}
	extraInfo := ""
	if db.tx != nil {
		extraInfo += " (tx)"
	}
	if retry {
		extraInfo += " (retry)"
	}
	for i := 0; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if file == "" || strings.Contains(file, "/sqlingo@v") {
			continue
		}
		segs := strings.Split(file, "/")
		name := segs[len(segs)-1]
		return fmt.Sprintf("/* %s:%d%s */ ", name, line, extraInfo)
	}
	return ""
}
