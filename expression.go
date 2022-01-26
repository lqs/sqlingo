package sqlingo

import (
	"fmt"
	"reflect"
	"strconv"
)

type priority uint8

// Expression is the interface of an SQL expression.
type Expression interface {
	// get the SQL string
	GetSQL(scope scope) (string, error)
	getOperatorPriority() priority

	// <> operator
	NotEquals(other interface{}) BooleanExpression
	// == operator
	Equals(other interface{}) BooleanExpression
	// < operator
	LessThan(other interface{}) BooleanExpression
	// <= operator
	LessThanOrEquals(other interface{}) BooleanExpression
	// > operator
	GreaterThan(other interface{}) BooleanExpression
	// >= operator
	GreaterThanOrEquals(other interface{}) BooleanExpression

	IsNull() BooleanExpression
	IsNotNull() BooleanExpression
	In(values ...interface{}) BooleanExpression
	NotIn(values ...interface{}) BooleanExpression
	Between(min interface{}, max interface{}) BooleanExpression
	NotBetween(min interface{}, max interface{}) BooleanExpression
	Desc() OrderBy

	As(alias string) Alias

	IfNull(altValue interface{}) Expression
}

// Alias is the interface of an table/column alias.
type Alias interface {
	GetSQL(scope scope) (string, error)
}

// BooleanExpression is the interface of an SQL expression with boolean value.
type BooleanExpression interface {
	Expression
	And(other interface{}) BooleanExpression
	Or(other interface{}) BooleanExpression
	Xor(other interface{}) BooleanExpression
	Not() BooleanExpression
}

// NumberExpression is the interface of an SQL expression with number value.
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

// StringExpression is the interface of an SQL expression with string value.
type StringExpression interface {
	Expression
	Min() UnknownExpression
	Max() UnknownExpression
	Like(other interface{}) BooleanExpression
	Contains(substring string) BooleanExpression
	Concat(other interface{}) StringExpression
	IfEmpty(altValue interface{}) StringExpression
	IsEmpty() BooleanExpression
}

// UnknownExpression is the interface of an SQL expression with unknown value.
type UnknownExpression interface {
	Expression
	And(other interface{}) BooleanExpression
	Or(other interface{}) BooleanExpression
	Xor(other interface{}) BooleanExpression
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

	Like(other interface{}) BooleanExpression
	Contains(substring string) BooleanExpression
	Concat(other interface{}) StringExpression
	IfEmpty(altValue interface{}) StringExpression
	IsEmpty() BooleanExpression
}

type expression struct {
	sql      string
	builder  func(scope scope) (string, error)
	priority priority
	isTrue   bool
	isFalse  bool
}

func (e expression) GetTable() Table {
	return nil
}

type scope struct {
	Database *database
	Tables   []Table
	lastJoin *join
}

func staticExpression(sql string, priority priority) expression {
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

// Raw create a raw SQL statement
func Raw(sql string) UnknownExpression {
	return expression{
		sql:      sql,
		priority: 99,
	}
}

// And creates an expression with AND operator.
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

// Or creates an expression with OR operator.
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

func (e expression) IfEmpty(altValue interface{}) StringExpression {
	return If(e.NotEquals(""), e, altValue)
}

func (e expression) IsEmpty() BooleanExpression {
	return e.Equals("")
}

func (e expression) GetSQL(scope scope) (string, error) {
	if e.sql != "" {
		return e.sql, nil
	}
	return e.builder(scope)
}

func quoteIdentifier(identifier string) (result dialectArray) {
	for dialect := dialect(0); dialect < dialectCount; dialect++ {
		switch dialect {
		case dialectMySQL:
			result[dialect] = "`" + identifier + "`"
		case dialectMSSQL:
			result[dialect] = "[" + identifier + "]"
		default:
			result[dialect] = "\"" + identifier + "\""
		}
	}
	return
}

func quoteString(s string) string {
	bytes := []byte(s)
	buf := make([]byte, len(s)*2+2)
	buf[0] = '\''
	n := 1

	for _, b := range bytes {
		switch b {
		case 0, '\n', '\r', '\\', '\'', '"', 0x1a:
			buf[n] = '\\'
			n++
		}
		buf[n] = b
		n++
	}
	buf[n] = '\''
	n++
	return string(buf[:n])
}

func getSQL(scope scope, value interface{}) (sql string, priority priority, err error) {
	if value == nil {
		sql = "NULL"
		return
	}
	switch value.(type) {
	case int:
		sql = strconv.Itoa(value.(int))
	case string:
		sql = quoteString(value.(string))
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
	case toUpdateFinal:
		sql, err = value.(toUpdateFinal).GetSQL()
	case Table:
		sql = value.(Table).GetSQL(scope)
	case CaseExpression:
		sql, err = value.(CaseExpression).End().GetSQL(scope)
	default:
		v := reflect.ValueOf(value)
		sql, priority, err = getSQLFromReflectValue(scope, v)
	}
	return
}

func getSQLFromReflectValue(scope scope, v reflect.Value) (sql string, priority priority, err error) {
	if v.Kind() == reflect.Ptr {
		// dereference pointers
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
		sql, priority, err = getSQL(scope, v.Interface())
		return
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
		sql = quoteString(v.String())
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
		if vs, ok := v.Interface().(interface{ String() string }); ok {
			sql = quoteString(vs.String())
		} else {
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

func toBooleanExpression(value interface{}) BooleanExpression {
	e, ok := value.(expression)
	switch {
	case !ok:
		return nil
	case e.isTrue:
		return trueExpression()
	case e.isFalse:
		return falseExpression()
	default:
		return nil
	}
}

func (e expression) And(other interface{}) BooleanExpression {
	switch {
	case e.isFalse:
		return e
	case e.isTrue:
		if exp := toBooleanExpression(other); exp != nil {
			return exp
		}
	}
	return e.binaryOperation("AND", other, 14)
}

func (e expression) Or(other interface{}) BooleanExpression {
	switch {
	case e.isTrue:
		return e
	case e.isFalse:
		if exp := toBooleanExpression(other); exp != nil {
			return exp
		}
	}
	return e.binaryOperation("OR", other, 16)
}

func (e expression) Xor(other interface{}) BooleanExpression {
	return e.binaryOperation("XOR", other, 15)
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

func (e expression) Like(other interface{}) BooleanExpression {
	return e.binaryOperation("LIKE", other, 11)
}

func (e expression) Concat(other interface{}) StringExpression {
	return Concat(e, other)
}

func (e expression) Contains(substring string) BooleanExpression {
	return function("LOCATE", substring, e).GreaterThan(0)
}

func (e expression) binaryOperation(operator string, value interface{}, priority priority) expression {
	return expression{builder: func(scope scope) (string, error) {
		leftSql, err := e.GetSQL(scope)
		if err != nil {
			return "", err
		}
		leftPriority := e.priority
		rightSql, rightPriority, err := getSQL(scope, value)
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

func (e expression) prefixSuffixExpression(prefix string, suffix string, priority priority) expression {
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

func expandSliceValue(value reflect.Value) (result []interface{}) {
	result = make([]interface{}, 0, 16)
	kind := value.Kind()
	switch kind {
	case reflect.Array, reflect.Slice:
		length := value.Len()
		for i := 0; i < length; i++ {
			result = append(result, expandSliceValue(value.Index(i))...)
		}
	case reflect.Interface, reflect.Ptr:
		result = append(result, expandSliceValue(value.Elem())...)
	default:
		result = append(result, value.Interface())
	}
	return
}

func expandSliceValues(values []interface{}) (result []interface{}) {
	result = make([]interface{}, 0, 16)
	for _, v := range values {
		value := reflect.ValueOf(v)
		result = append(result, expandSliceValue(value)...)
	}
	return
}

func (e expression) In(values ...interface{}) BooleanExpression {
	values = expandSliceValues(values)
	if len(values) == 0 {
		return falseExpression()
	}
	joiner := func(exprSql, valuesSql string) string { return exprSql + " IN (" + valuesSql + ")" }
	builder := e.getBuilder(e.Equals, joiner, values...)
	return expression{builder: builder, priority: 11}
}

func (e expression) NotIn(values ...interface{}) BooleanExpression {
	values = expandSliceValues(values)
	if len(values) == 0 {
		return trueExpression()
	}
	joiner := func(exprSql, valuesSql string) string { return exprSql + " NOT IN (" + valuesSql + ")" }
	builder := e.getBuilder(e.NotEquals, joiner, values...)
	return expression{builder: builder, priority: 11}
}

type joinerFunc = func(exprSql, valuesSql string) string
type booleanFunc = func(other interface{}) BooleanExpression
type builderFunc = func(scope scope) (string, error)

func (e expression) getBuilder(single booleanFunc, joiner joinerFunc, values ...interface{}) builderFunc {
	return func(scope scope) (string, error) {
		var valuesSql string
		var err error

		if len(values) == 1 {
			value := values[0]
			if selectStatus, ok := value.(toSelectFinal); ok {
				// IN subquery
				valuesSql, err = selectStatus.GetSQL()
				if err != nil {
					return "", err
				}
			} else {
				// IN a single value
				return single(value).GetSQL(scope)
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
		return joiner(exprSql, valuesSql), nil
	}
}

func (e expression) Between(min interface{}, max interface{}) BooleanExpression {
	return e.buildBetween(" BETWEEN ", min, max)
}

func (e expression) NotBetween(min interface{}, max interface{}) BooleanExpression {
	return e.buildBetween(" NOT BETWEEN ", min, max)
}

func (e expression) buildBetween(operator string, min interface{}, max interface{}) BooleanExpression {
	return expression{builder: func(scope scope) (string, error) {
		exprSql, err := e.GetSQL(scope)
		if err != nil {
			return "", err
		}
		minSql, _, err := getSQL(scope, min)
		if err != nil {
			return "", err
		}
		maxSql, _, err := getSQL(scope, max)
		if err != nil {
			return "", err
		}
		return exprSql + operator + minSql + " AND " + maxSql, nil
	}, priority: 12}
}

func (e expression) getOperatorPriority() priority {
	return e.priority
}

func (e expression) Desc() OrderBy {
	return orderBy{by: e, desc: true}
}
