package sqlingo

import (
	"errors"
	"testing"
)

func TestFunction(t *testing.T) {
	a1 := expression{sql: "a1"}
	a2 := expression{sql: "a2"}
	ee := expression{builder: func(scope scope) (string, error) {
		return "", errors.New("error")
	}}

	assertValue(t, Function("func"), "func()")
	assertValue(t, Function("func", a1), "func(a1)")
	assertValue(t, Function("func", a1, a2), "func(a1, a2)")
	assertError(t, Function("func", a1, ee))

	assertValue(t, Concat(a1, a2), "CONCAT(a1, a2)")
	assertValue(t, Count(a1), "COUNT(a1)")
	assertValue(t, If(a1, 1, 2), "IF(a1, 1, 2)")
	assertValue(t, Length(a1), "LENGTH(a1)")
}
