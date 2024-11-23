package sqlingo

import "strings"

func appendWhere(sb *strings.Builder, scope scope, where BooleanExpression) error {
	if where == nil {
		return nil
	}
	if e, ok := where.(expression); ok {
		if e.isTrue {
			return nil
		} else if e.isFalse {
			sb.WriteString(" WHERE FALSE")
			return nil
		}
	}

	whereSql, err := where.GetSQL(scope)
	if err != nil {
		return err
	}
	sb.WriteString(" WHERE ")
	sb.WriteString(whereSql)
	return nil
}
