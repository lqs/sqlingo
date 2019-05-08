package sqlingo

import (
	"errors"
	"testing"
)

func TestOrder(t *testing.T) {
	e := expression{sql: "x"}
	assertValue(t, orderBy{by: e}, "x")
	assertValue(t, orderBy{by: e, desc:true}, "x DESC")
	assertError(t, orderBy{by: expression{builder: func(scope scope) (string, error) {
		return "", errors.New("error")
	}}})
}
