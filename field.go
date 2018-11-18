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

func newFieldExpression(tableName string, fieldName string) *expression {
	sql := getSQLForName(fieldName)
	if tableName != "" {
		sql = getSQLForName(tableName) + "." + sql
	}
	return &expression{sql: sql, priority: 0}
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
