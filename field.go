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
	shortFieldNameSql := getSQLForName(fieldName)
	fullFieldNameSql := getSQLForName(tableName) + "." + shortFieldNameSql
	return expression{builder: func(scope scope) (string, error) {
		if len(scope.Tables) != 1 || scope.lastJoin != nil || scope.Tables[0].GetName() != tableName {
			return fullFieldNameSql, nil
		}
		return shortFieldNameSql, nil
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
