package sqlingo

import (
	"errors"
	"testing"
)

type CustomInt int
type CustomBool bool
type CustomFloat float32
type CustomString string

func TestExpression(t *testing.T) {
	assertValue(t, nil, "NULL")

	assertValue(t, false, "0")
	assertValue(t, true, "1")
	assertValue(t, CustomBool(false), "0")
	assertValue(t, CustomBool(true), "1")

	assertValue(t, int8(11), "11")
	assertValue(t, int16(11111), "11111")
	assertValue(t, int32(1111111111), "1111111111")
	assertValue(t, CustomInt(1111111111), "1111111111")
	assertValue(t, int(1111111111), "1111111111")
	assertValue(t, int64(1111111111111111111), "1111111111111111111")

	assertValue(t, int8(-11), "-11")
	assertValue(t, int16(-11111), "-11111")
	assertValue(t, int32(-1111111111), "-1111111111")
	assertValue(t, CustomInt(-1111111111), "-1111111111")
	assertValue(t, int(-1111111111), "-1111111111")
	assertValue(t, int64(-1111111111111111111), "-1111111111111111111")

	assertValue(t, uint8(1), "1")
	assertValue(t, uint16(55555), "55555")
	assertValue(t, uint32(3333333333), "3333333333")
	assertValue(t, uint(3333333333), "3333333333")
	assertValue(t, uint64(11111111111111111111), "11111111111111111111")

	assertValue(t, float32(2), "2")
	assertValue(t, float32(-2), "-2")
	assertValue(t, float64(2), "2")
	assertValue(t, float64(-2), "-2")

	assertValue(t, "abc", "'abc'")
	assertValue(t, "", "''")
	assertValue(t, "a' or 'a'='a", "'a\\' or \\'a\\'=\\'a'")
	assertValue(t, "\n", "'\\\n'")
	assertValue(t, CustomString("abc"), "'abc'")

	x := 3
	px := &x
	ppx := &px
	var deepNil *****int
	assertValue(t, &x, "3")
	assertValue(t, &px, "3")
	assertValue(t, &ppx, "3")
	assertValue(t, deepNil, "NULL")
}

func TestFunc(t *testing.T) {
	e := expression{
		builder: func(scope scope) (string, error) {
			return "<>", nil
		},
	}

	assertValue(t, e.Equals(e), "<> = <>")
	assertValue(t, e.NotEquals(e), "<> <> <>")
	assertValue(t, e.LessThan(e), "<> < <>")
	assertValue(t, e.LessThanOrEquals(e), "<> <= <>")
	assertValue(t, e.GreaterThan(e), "<> > <>")
	assertValue(t, e.GreaterThanOrEquals(e), "<> >= <>")
	assertValue(t, e.And(e), "<> AND <>")
	assertValue(t, e.Or(e), "<> OR <>")
	assertValue(t, e.Xor(e), "<> XOR <>")
	assertValue(t, e.Not(), "NOT <>")

	assertValue(t, e.Add(e), "<> + <>")
	assertValue(t, e.Sub(e), "<> - <>")
	assertValue(t, e.Mul(e), "<> * <>")
	assertValue(t, e.Div(e), "<> / <>")
	assertValue(t, e.IntDiv(e), "<> DIV <>")
	assertValue(t, e.Mod(e), "<> % <>")
	assertValue(t, e.Sum(), "SUM(<>)")
	assertValue(t, e.Avg(), "AVG(<>)")
	assertValue(t, e.Min(), "MIN(<>)")
	assertValue(t, e.Max(), "MAX(<>)")
	assertValue(t, e.Between(2, 4), "<> BETWEEN 2 AND 4")
	assertValue(t, e.NotBetween(2, 4), "<> NOT BETWEEN 2 AND 4")

	assertValue(t, e.In(), "FALSE")
	assertValue(t, e.In(1), "<> = 1")
	assertValue(t, e.In(1, 2, 3), "<> IN (1, 2, 3)")
	assertValue(t, e.In([]int64{}), "FALSE")
	assertValue(t, e.In([]int64{1}), "<> = 1")
	assertValue(t, e.In([]int64{1, 2, 3}), "<> IN (1, 2, 3)")
	assertValue(t, e.In([]byte{1, 2, 3}), "<> IN (1, 2, 3)")

	assertValue(t, e.NotIn(), "TRUE")
	assertValue(t, e.NotIn(1), "<> <> 1")
	assertValue(t, e.NotIn(1, 2, 3), "<> NOT IN (1, 2, 3)")
	assertValue(t, e.NotIn([]int64{}), "TRUE")
	assertValue(t, e.NotIn([]int64{1}), "<> <> 1")
	assertValue(t, e.NotIn([]int64{1, 2, 3}), "<> NOT IN (1, 2, 3)")

	assertValue(t, e.Like("%A%"), "<> LIKE '%A%'")
	assertValue(t, e.Concat("-suffix"), "CONCAT(<>, '-suffix')")
	assertValue(t, e.Contains("\n"), "LOCATE('\\\n', <>) > 0")

	assertValue(t, []interface{}{1, 2, 3, "d"}, "(1, 2, 3, 'd')")

	assertValue(t, e.IsNull(), "<> IS NULL")
	assertValue(t, e.IsNotNull(), "<> IS NOT NULL")
	assertValue(t, e.IsTrue(), "<> IS TRUE")
	assertValue(t, e.IsNotTrue(), "<> IS NOT TRUE")
	assertValue(t, e.IsFalse(), "<> IS FALSE")
	assertValue(t, e.IsNotFalse(), "<> IS NOT FALSE")
	assertValue(t, e.If(3, 4), "IF(<>, 3, 4)")
	assertValue(t, e.IfNull(3), "IFNULL(<>, 3)")
	assertValue(t, e.IfEmpty(3), "IF(<> <> '', <>, 3)")
	assertValue(t, e.IsEmpty(), "<> = ''")
	assertValue(t, e.Lower(), "LOWER(<>)")
	assertValue(t, e.Upper(), "UPPER(<>)")
	assertValue(t, e.Left(10), "LEFT(<>, 10)")
	assertValue(t, e.Right(10), "RIGHT(<>, 10)")
	assertValue(t, e.Trim(), "TRIM(<>)")
	assertValue(t, e.HasPrefix("abc"), "LEFT(<>, CHAR_LENGTH('abc')) = 'abc'")
	assertValue(t, e.HasSuffix("abc"), "RIGHT(<>, CHAR_LENGTH('abc')) = 'abc'")

	e5 := expression{
		builder: func(scope scope) (string, error) {
			return "e5", nil
		},
		priority: 5,
	}
	e7 := expression{
		builder: func(scope scope) (string, error) {
			return "e7", nil
		},
		priority: 7,
	}
	e9 := expression{
		builder: func(scope scope) (string, error) {
			return "e9", nil
		},
		priority: 9,
	}

	assertValue(t, e7.Add(e7), "e7 + (e7)")
	assertValue(t, e5.Add(e7), "e5 + (e7)")
	assertValue(t, e7.Add(e5), "e7 + e5")
	assertValue(t, e5.Add(e9), "e5 + (e9)")
	assertValue(t, e9.Add(e5), "(e9) + e5")

	ee := expression{
		builder: func(scope scope) (string, error) {
			return "", errors.New("error")
		},
	}
	assertError(t, e.Add(ee))
	assertError(t, ee.Add(e))
	assertError(t, ee.IsNull())
	assertError(t, e.In(ee, ee, ee))
	assertError(t, ee.In(e, e, e))

	assertError(t, ee.Between(2, 4))
	assertError(t, e.Between(2, ee))
	assertError(t, e.Between(ee, 4))

}

func TestMisc(t *testing.T) {
	assertValue(t, True(), "TRUE")
	assertValue(t, False(), "FALSE")

	assertValue(t, command("COMMAND", staticExpression("<arg>", 0, false)), "COMMAND <arg>")

	assertValue(t, staticExpression("<expression>", 1, false).
		prefixSuffixExpression("<prefix>", "<suffix>", 1, false), "<prefix><expression><suffix>")
}

func TestLogicalExpression(t *testing.T) {
	a := expression{sql: "a", priority: 1}
	b := expression{sql: "b", priority: 1}
	c := expression{sql: "c", priority: 1}
	d := expression{sql: "d", priority: 1}

	assertValue(t, And(a, b, c, d), "a AND b AND c AND d")
	assertValue(t, Or(a, b, c, d), "a OR b OR c OR d")
	assertValue(t, a.And(b).Or(c).And(a).Or(b).And(c), "((a AND b OR c) AND a OR b) AND c")
	assertValue(t, a.Or(b).And(c.Or(d)), "(a OR b) AND (c OR d)")
	assertValue(t, a.Or(b).And(c).Not(), "NOT ((a OR b) AND c)")

	assertValue(t, And(), "TRUE")
	assertValue(t, Or(), "FALSE")
}

func TestLogicalOptimizer(t *testing.T) {
	trueValue := True()
	falseValue := False()
	otherValue := staticExpression("<>", 0, false)
	otherBoolValue := staticExpression("<>", 0, true)

	assertValue(t, trueValue.Or(trueValue), "TRUE")
	assertValue(t, trueValue.Or(falseValue), "TRUE")
	assertValue(t, falseValue.Or(trueValue), "TRUE")
	assertValue(t, falseValue.Or(falseValue), "FALSE")

	assertValue(t, trueValue.And(trueValue), "TRUE")
	assertValue(t, trueValue.And(falseValue), "FALSE")
	assertValue(t, falseValue.And(trueValue), "FALSE")
	assertValue(t, falseValue.And(falseValue), "FALSE")

	assertValue(t, falseValue.Not(), "TRUE")
	assertValue(t, trueValue.Not(), "FALSE")

	assertValue(t, trueValue.And(otherValue), "TRUE AND <>")
	assertValue(t, trueValue.Or(otherValue), "TRUE")
	assertValue(t, trueValue.And(123), "TRUE AND 123")
	assertValue(t, trueValue.Or(123), "TRUE")
	assertValue(t, falseValue.And(otherValue), "FALSE")
	assertValue(t, falseValue.Or(otherValue), "FALSE OR <>")
	assertValue(t, falseValue.And(123), "FALSE")
	assertValue(t, falseValue.Or(123), "FALSE OR 123")

	assertValue(t, trueValue.And(otherBoolValue), "<>")
	assertValue(t, falseValue.Or(otherBoolValue), "<>")
}
