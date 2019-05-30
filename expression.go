package sqlingo

import (
	"fmt"
	"reflect"
	"strconv"
)

type Expression interface {
	GetSQL(scope scope) (string, error)
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

type Alias interface {
	GetSQL(scope scope) (string, error)
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
	Avg() NumberExpression
	Min() UnknownExpression
	Max() UnknownExpression
}

type StringExpression interface {
	Expression
	Min() UnknownExpression
	Max() UnknownExpression
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
	Avg() NumberExpression
	Min() UnknownExpression
	Max() UnknownExpression
}

type expression struct {
	sql      string
	builder  func(scope scope) (string, error)
	priority int
	isTrue   bool
	isFalse  bool
}

type scope struct {
	Database *database
	Tables   []Table
	lastJoin *join
}

func staticExpression(sql string, priority int) expression {
	return expression{
		sql:      sql,
		priority: priority,
	}
}

func trueExpression() expression {
	return expression{
		sql:    "1",
		isTrue: true,
	}
}

func falseExpression() expression {
	return expression{
		sql:     "0",
		isFalse: true,
	}
}

func (e expression) As(name string) Alias {
	return expression{builder: func(scope scope) (string, error) {
		expressionSql, err := e.GetSQL(scope)
		if err != nil {
			return "", err
		}
		return expressionSql + " AS " + name, nil
	}}
}

func (e expression) IfNull(altValue interface{}) Expression {
	return Function("IFNULL", e, altValue)
}

func (e expression) GetSQL(scope scope) (string, error) {
	if e.sql != "" {
		return e.sql, nil
	}
	return e.builder(scope)
}

func escape(s string) string {
	bytes := []byte(s)
	n := 0
	buf := make([]byte, len(s)*2)

	for _, b := range bytes {
		switch b {
		case 0, '\n', '\r', '\\', '\'', '"', 0x1a:
			buf[n] = '\\'
			n++
		}
		buf[n] = b
		n++
	}
	return string(buf[:n])
}

func getSQLFromWhatever(scope scope, value interface{}) (sql string, priority int, err error) {
	if value == nil {
		sql = "NULL"
		return
	}
	switch value.(type) {
	case Expression:
		sql, err = value.(Expression).GetSQL(scope)
		priority = value.(Expression).getOperatorPriority()
	case Assignment:
		sql, err = value.(Assignment).GetSQL(scope)
	case toSelectFinal:
		sql, err = value.(toSelectFinal).GetSQL()
		if err != nil {
			return
		}
		sql = "(" + sql + ")"
	case Table:
		sql = value.(Table).GetSQL(scope)
	case CaseExpression:
		sql, err = value.(CaseExpression).End().GetSQL(scope)
	default:
		v := reflect.ValueOf(value)
		if v.Kind() == reflect.Ptr {
			for {
				if v.IsNil() {
					sql = "NULL"
					return
				}
				v = v.Elem()
				if v.Kind() != reflect.Ptr {
					break
				}
			}
			return getSQLFromWhatever(scope, v.Interface())
		}

		switch v.Kind() {
		case reflect.Bool:
			if v.Bool() {
				sql = "1"
			} else {
				sql = "0"
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			sql = strconv.FormatInt(v.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			sql = strconv.FormatUint(v.Uint(), 10)
		case reflect.Float32, reflect.Float64:
			sql = strconv.FormatFloat(v.Float(), 'g', -1, 64)
		case reflect.String:
			sql = "\"" + escape(v.String()) + "\""
		case reflect.Array, reflect.Slice:
			length := v.Len()
			values := make([]interface{}, length)
			for i := 0; i < length; i++ {
				values[i] = v.Index(i).Interface()
			}
			sql, err = commaValues(scope, values)
			if err == nil {
				sql = "(" + sql + ")"
			}
		default:
			err = fmt.Errorf("invalid type %s", v.Kind().String())
		}
	}
	return
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
func (e expression) NotEquals(other interface{}) BooleanExpression {
	return e.binaryOperation("<>", other, 11)
}

func (e expression) Equals(other interface{}) BooleanExpression {
	return e.binaryOperation("=", other, 11)
}

func (e expression) LessThan(other interface{}) BooleanExpression {
	return e.binaryOperation("<", other, 11)
}

func (e expression) LessThanOrEquals(other interface{}) BooleanExpression {
	return e.binaryOperation("<=", other, 11)
}

func (e expression) GreaterThan(other interface{}) BooleanExpression {
	return e.binaryOperation(">", other, 11)
}

func (e expression) GreaterThanOrEquals(other interface{}) BooleanExpression {
	return e.binaryOperation(">=", other, 11)
}

func (e expression) And(other interface{}) BooleanExpression {
	if e.isFalse {
		return e
	}
	return e.binaryOperation("AND", other, 14)
}

func (e expression) Or(other interface{}) BooleanExpression {
	if e.isTrue {
		return e
	}
	return e.binaryOperation("OR", other, 16)
}

func (e expression) Add(other interface{}) NumberExpression {
	return e.binaryOperation("+", other, 7)
}

func (e expression) Sub(other interface{}) NumberExpression {
	return e.binaryOperation("-", other, 7)
}

func (e expression) Mul(other interface{}) NumberExpression {
	return e.binaryOperation("*", other, 6)
}

func (e expression) Div(other interface{}) NumberExpression {
	return e.binaryOperation("/", other, 6)
}

func (e expression) IntDiv(other interface{}) NumberExpression {
	return e.binaryOperation("DIV", other, 6)
}

func (e expression) Mod(other interface{}) NumberExpression {
	return e.binaryOperation("%", other, 6)
}

func (e expression) Sum() NumberExpression {
	return function("SUM", e)
}

func (e expression) Avg() NumberExpression {
	return function("AVG", e)
}

func (e expression) Min() UnknownExpression {
	return function("MIN", e)
}

func (e expression) Max() UnknownExpression {
	return function("MAX", e)
}

func (e expression) binaryOperation(operator string, value interface{}, priority int) expression {
	return expression{builder: func(scope scope) (string, error) {
		leftSql, err := e.GetSQL(scope)
		if err != nil {
			return "", err
		}
		leftPriority := e.priority
		rightSql, rightPriority, err := getSQLFromWhatever(scope, value)
		if err != nil {
			return "", err
		}
		if leftPriority > priority {
			leftSql = "(" + leftSql + ")"
		}
		if rightPriority >= priority {
			rightSql = "(" + rightSql + ")"
		}
		return leftSql + " " + operator + " " + rightSql, err
	}, priority: priority}
}

func (e expression) prefixSuffixExpression(prefix string, suffix string, priority int) expression {
	if e.sql != "" {
		return expression{
			sql:      prefix + e.sql + suffix,
			priority: priority,
		}
	}
	return expression{builder: func(scope scope) (string, error) {
		exprSql, err := e.GetSQL(scope)
		if err != nil {
			return "", err
		}
		return prefix + exprSql + suffix, nil
	}, priority: priority}
}

func (e expression) IsNull() BooleanExpression {
	return e.prefixSuffixExpression("", " IS NULL", 11)
}

func (e expression) Not() BooleanExpression {
	if e.isTrue {
		return falseExpression()
	}
	if e.isFalse {
		return trueExpression()
	}
	return e.prefixSuffixExpression("NOT ", "", 13)
}

func (e expression) IsNotNull() BooleanExpression {
	return e.prefixSuffixExpression("", " IS NOT NULL", 11)
}

func (e expression) In(values ...interface{}) BooleanExpression {
	if len(values) == 1 {
		value := reflect.ValueOf(values[0])
		kind := value.Kind()
		if kind == reflect.Array || kind == reflect.Slice {
			length := value.Len()
			values = make([]interface{}, length)
			for i := 0; i < length; i++ {
				values[i] = value.Index(i).Interface()
			}
		}
	}
	if len(values) == 0 {
		return falseExpression()
	}
	return expression{builder: func(scope scope) (string, error) {

		var valuesSql string
		var err error

		if len(values) == 1 {
			value := values[0]
			if select_, ok := value.(toSelectFinal); ok {
				// IN subquery
				valuesSql, err = select_.GetSQL()
				if err != nil {
					return "", err
				}
			} else {
				// IN a single value
				return e.Equals(value).GetSQL(scope)
			}
		} else {
			// IN a list
			valuesSql, err = commaValues(scope, values)
			if err != nil {
				return "", err
			}
		}

		exprSql, err := e.GetSQL(scope)
		if err != nil {
			return "", err
		}

		return exprSql + " IN (" + valuesSql + ")", nil
	}, priority: 11}
}

func (e expression) Between(min interface{}, max interface{}) BooleanExpression {
	return expression{builder: func(scope scope) (string, error) {
		exprSql, err := e.GetSQL(scope)
		if err != nil {
			return "", err
		}
		minSql, _, err := getSQLFromWhatever(scope, min)
		if err != nil {
			return "", err
		}
		maxSql, _, err := getSQLFromWhatever(scope, max)
		if err != nil {
			return "", err
		}
		return exprSql + " BETWEEN " + minSql + " AND " + maxSql, nil
	}, priority: 12}

}

func (e expression) getOperatorPriority() int {
	return e.priority
}

func (e expression) Desc() OrderBy {
	return orderBy{by: e, desc: true}
}
