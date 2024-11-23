package sqlingo

import (
	"strings"
	"testing"
)

func buildWhere(where BooleanExpression) string {
	var sb strings.Builder
	scope := scope{}
	if err := appendWhere(&sb, scope, where); err != nil {
		panic(err)
	}
	return sb.String()
}

func TestAppendWhere(t *testing.T) {
	assertEqual(t, buildWhere(True()), "")
	assertEqual(t, buildWhere(False()), " WHERE FALSE")
	assertEqual(t, buildWhere(Raw("##")), " WHERE ##")
}
