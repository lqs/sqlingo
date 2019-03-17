package sqlingo

type Field interface {
	Expression
}

type NumberField interface {
	NumberExpression
}

type BooleanField interface {
	BooleanExpression
}

type StringField interface {
	StringExpression
}

func newFieldExpression(tableName string, fieldName string) expression {
	return expression{builder: func(scope scope) (string, error) {
		sql := getSQLForName(fieldName)
		if len(scope.Tables) != 1 || scope.lastJoin != nil || scope.Tables[0].GetName() != tableName {
			sql = getSQLForName(tableName) + "." + sql
		}
		return sql, nil
	}}
}

func NewNumberField(tableName string, fieldName string) NumberField {
	return newFieldExpression(tableName, fieldName)
}

func NewBooleanField(tableName string, fieldName string) BooleanField {
	return newFieldExpression(tableName, fieldName)
}

func NewStringField(tableName string, fieldName string) StringField {
	return newFieldExpression(tableName, fieldName)
}
