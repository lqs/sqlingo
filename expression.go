package sqlingo

import (
	"reflect"
	"strconv"
	"strings"
)

type Expression interface {
	GetSQL() string
	getOperatorPriority() int

	NotEquals(other interface{}) BooleanExpression
	Equals(other interface{}) BooleanExpression
	LessThan(other interface{}) BooleanExpression
	LessThanOrEquals(other interface{}) BooleanExpression
	GreaterThan(other interface{}) BooleanExpression
	GreaterThanOrEquals(other interface{}) BooleanExpression

	IsNull() BooleanExpression
	IsNotNull() BooleanExpression
	In(values ...interface{}) BooleanExpression
	Between(min interface{}, max interface{}) BooleanExpression
	Desc() OrderBy

	As(alias string) Alias

	IfNull(altValue interface{}) Expression
}

type BooleanExpression interface {
	Expression
	And(other interface{}) BooleanExpression
	Or(other interface{}) BooleanExpression
	Not() BooleanExpression
}

type NumberExpression interface {
	Expression
	Add(other interface{}) NumberExpression
	Sub(other interface{}) NumberExpression
	Mul(other interface{}) NumberExpression
	Div(other interface{}) NumberExpression
	IntDiv(other interface{}) NumberExpression
	Mod(other interface{}) NumberExpression

	Sum() NumberExpression
}

type StringExpression interface {
	Expression
}

type UnknownExpression interface {
	Expression
	And(other interface{}) BooleanExpression
	Or(other interface{}) BooleanExpression
	Not() BooleanExpression
	Add(other interface{}) NumberExpression
	Sub(other interface{}) NumberExpression
	Mul(other interface{}) NumberExpression
	Div(other interface{}) NumberExpression
	IntDiv(other interface{}) NumberExpression
	Mod(other interface{}) NumberExpression

	Sum() NumberExpression
}

type expression struct {
	sql      string
	priority int
}

func (e *expression) As(name string) Alias {
	return &alias{expression: e, name: name}
}

func (e *expression) IfNull(altValue interface{}) Expression {
	return Function("IFNULL", e, altValue)
}

func (e *expression) GetSQL() string {
	return e.sql
}

func getSQLFromWhatever(value interface{}) (sql string, priority int) {
	switch value.(type) {
	case Expression:
		return value.(Expression).GetSQL(), value.(Expression).getOperatorPriority()
	case Assignment:
		return value.(Assignment).GetSQL(), 0
	case int, int8, int16, int32, int64:
		return strconv.FormatInt(reflect.ValueOf(value).Int(), 10), 0
	case uint, uint8, uint16, uint32, uint64:
		return strconv.FormatUint(reflect.ValueOf(value).Uint(), 10), 0
	case string:
		return "\"" + strings.Replace(value.(string), "\"", "\\\"", -1) + "\"", 0
	case []interface{}:
		return "(" + commaValues(value.([]interface{})) + ")", 0
	default:
		if value == nil {
			return "NULL", 0
		}
		v := reflect.ValueOf(value)
		for v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return "NULL", 0
			}
			return getSQLFromWhatever(reflect.Indirect(v).Interface())
		}
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return getSQLFromWhatever(v.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return getSQLFromWhatever(v.Uint())
		default:
			return "[invalid type " + v.Kind().String() + "]", 99
		}
	}
}

/*
1 INTERVAL
2 BINARY, COLLATE
3 !
4 - (unary minus), ~ (unary bit inversion)
5 ^
6 *, /, DIV, %, MOD
7 -, +
8 <<, >>
9 &
10 |
11 = (comparison), <=>, >=, >, <=, <, <>, !=, IS, LIKE, REGEXP, IN
12 BETWEEN, CASE, WHEN, THEN, ELSE
13 NOT
14 AND, &&
15 XOR
16 OR, ||
17 = (assignment), :=
*/
func (e *expression) NotEquals(other interface{}) BooleanExpression {
	return e.binaryOperation("<>", other, 11)
}

func (e *expression) Equals(other interface{}) BooleanExpression {
	return e.binaryOperation("=", other, 11)
}

func (e *expression) LessThan(other interface{}) BooleanExpression {
	return e.binaryOperation("<", other, 11)
}

func (e *expression) LessThanOrEquals(other interface{}) BooleanExpression {
	return e.binaryOperation("<=", other, 11)
}

func (e *expression) GreaterThan(other interface{}) BooleanExpression {
	return e.binaryOperation(">", other, 11)
}

func (e *expression) GreaterThanOrEquals(other interface{}) BooleanExpression {
	return e.binaryOperation(">=", other, 11)
}

func (e *expression) And(other interface{}) BooleanExpression {
	return e.binaryOperation("AND", other, 14)
}

func (e *expression) Or(other interface{}) BooleanExpression {
	return e.binaryOperation("OR", other, 16)
}

func (e *expression) Add(other interface{}) NumberExpression {
	return e.binaryOperation("+", other, 7)
}

func (e *expression) Sub(other interface{}) NumberExpression {
	return e.binaryOperation("-", other, 7)
}

func (e *expression) Mul(other interface{}) NumberExpression {
	return e.binaryOperation("*", other, 6)
}

func (e *expression) Div(other interface{}) NumberExpression {
	return e.binaryOperation("/", other, 6)
}

func (e *expression) IntDiv(other interface{}) NumberExpression {
	return e.binaryOperation("DIV", other, 6)
}

func (e *expression) Mod(other interface{}) NumberExpression {
	return e.binaryOperation("%", other, 6)
}

func (e *expression) Sum() NumberExpression {
	return e.function("SUM")
}

func (e *expression) binaryOperation(operator string, value interface{}, priority int) *expression {
	left := e.GetSQL()
	leftLevel := e.priority
	right, rightLevel := getSQLFromWhatever(value)
	if leftLevel > priority {
		left = "(" + left + ")"
	}
	if rightLevel >= priority {
		right = "(" + right + ")"
	}
	return &expression{
		sql:      left + " " + operator + " " + right,
		priority: priority,
	}
}

func (e *expression) function(name string) *expression {
	return &expression{sql: name + "(" + e.GetSQL() + ")", priority: 0}
}

func (e *expression) IsNull() BooleanExpression {
	return &expression{sql: e.GetSQL() + " IS NULL", priority: 11}
}

func (e *expression) Not() BooleanExpression {
	return &expression{sql: "NOT " + e.GetSQL(), priority: 13}
}

func (e *expression) IsNotNull() BooleanExpression {
	return &expression{sql: e.GetSQL() + " IS NOT NULL", priority: 11}
}

func (e *expression) In(values ...interface{}) BooleanExpression {
	if len(values) == 1 {
		firstValue := values[0]
		valueOfFirstValue := reflect.ValueOf(firstValue)
		if valueOfFirstValue.Kind() == reflect.Slice {
			length := valueOfFirstValue.Len()
			values = make([]interface{}, length)
			for i := 0; i < length; i++ {
				value := valueOfFirstValue.Index(i)
				values[i] = value.Interface()
			}
		}
	}
	if len(values) == 0 {
		return &expression{sql: "FALSE", priority: 0}
	}

	sql := e.GetSQL() + " IN ("
	for i, value := range values {
		if i > 0 {
			sql += ", "
		}
		valueSql, _ := getSQLFromWhatever(value)
		sql += valueSql
	}
	sql += ")"
	return &expression{sql: sql, priority: 11}
}

func (e *expression) Between(min interface{}, max interface{}) BooleanExpression {
	minSql, _ := getSQLFromWhatever(min)
	maxSql, _ := getSQLFromWhatever(max)
	sql := e.GetSQL() + " BETWEEN " + minSql + " AND " + maxSql
	return &expression{sql: sql, priority: 12}
}

func (e *expression) getOperatorPriority() int {
	return e.priority
}

func (e *expression) Desc() OrderBy {
	return &orderBy{by: e, desc: true}
}

func NewExpression(sql string, priority int) {
}
