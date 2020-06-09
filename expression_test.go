package sqlingo

import (
	"errors"
	"testing"
)

func TestExpression(t *testing.T) {
	assertValue(t, nil, "NULL")

	assertValue(t, false, "0")
	assertValue(t, true, "1")

	assertValue(t, int8(11), "11")
	assertValue(t, int16(11111), "11111")
	assertValue(t, int32(1111111111), "1111111111")
	assertValue(t, int(1111111111), "1111111111")
	assertValue(t, int64(1111111111111111111), "1111111111111111111")

	assertValue(t, int8(-11), "-11")
	assertValue(t, int16(-11111), "-11111")
	assertValue(t, int32(-1111111111), "-1111111111")
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

	assertValue(t, "abc", "\"abc\"")
	assertValue(t, "", "\"\"")
	assertValue(t, "a' or 'a'='a", "\"a\\' or \\'a\\'=\\'a\"")
	assertValue(t, "\n", "\"\\\n\"")

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

	assertValue(t, e.In(), "0")
	assertValue(t, e.In(1), "<> = 1")
	assertValue(t, e.In(1, 2, 3), "<> IN (1, 2, 3)")
	assertValue(t, e.In([]int64{}), "0")
	assertValue(t, e.In([]int64{1}), "<> = 1")
	assertValue(t, e.In([]int64{1, 2, 3}), "<> IN (1, 2, 3)")

	assertValue(t, e.NotIn(), "1")
	assertValue(t, e.NotIn(1), "<> <> 1")
	assertValue(t, e.NotIn(1, 2, 3), "<> NOT IN (1, 2, 3)")
	assertValue(t, e.NotIn([]int64{}), "1")
	assertValue(t, e.NotIn([]int64{1}), "<> <> 1")
	assertValue(t, e.NotIn([]int64{1, 2, 3}), "<> NOT IN (1, 2, 3)")

	assertValue(t, e.Like("%A%"), "<> LIKE \"%A%\"")
	assertValue(t, e.Contains("\n"), "LOCATE(\"\\\n\", <>) > 0")

	assertValue(t, []interface{}{1, 2, 3, "d"}, "(1, 2, 3, \"d\")")

	assertValue(t, e.IsNull(), "<> IS NULL")
	assertValue(t, e.IsNotNull(), "<> IS NOT NULL")
	assertValue(t, e.IfNull(3), "IFNULL(<>, 3)")

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
}

func TestLogicalOptimizer(t *testing.T) {
	trueValue := And()
	falseValue := Or()

	assertValue(t, trueValue.Or(trueValue), "1")
	assertValue(t, trueValue.Or(falseValue), "1")
	assertValue(t, falseValue.Or(trueValue), "0 OR 1")
	assertValue(t, falseValue.Or(falseValue), "0 OR 0")

	assertValue(t, trueValue.And(trueValue), "1 AND 1")
	assertValue(t, trueValue.And(falseValue), "1 AND 0")
	assertValue(t, falseValue.And(trueValue), "0")
	assertValue(t, falseValue.And(falseValue), "0")
}
