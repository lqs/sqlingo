package sqlingo

import (
	"errors"
	"testing"
)

func TestCase(t *testing.T) {
	c1 := expression{
		builder: func(scope scope) (string, error) {
			return "c1", nil
		},
	}
	c2 := expression{
		builder: func(scope scope) (string, error) {
			return "c2", nil
		},
	}
	assertValue(t, Case().WhenThen(c1, 1).WhenThen(c2, 2),
		"CASE WHEN c1 THEN 1 WHEN c2 THEN 2 END")
	assertValue(t, Case().WhenThen(c1, 1).WhenThen(c2, 2).Else(0),
		"CASE WHEN c1 THEN 1 WHEN c2 THEN 2 ELSE 0 END")
	assertValue(t, Case().Else(0),
		"0")

	ee := expression{
		builder: func(scope scope) (string, error) {
			return "", errors.New("error")
		},
	}
	assertError(t, Case().WhenThen(ee, 2))
	assertError(t, Case().WhenThen(c1, ee))
	assertError(t, Case().WhenThen(c1, 1).Else(ee))
}
