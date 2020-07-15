package sqlingo

import "strings"

// Field is the interface of a generated field.
type Field interface {
	Expression
}

// NumberField is the interface of a generated field of number type.
type NumberField interface {
	NumberExpression
}

// BooleanField is the interface of a generated field of boolean type.
type BooleanField interface {
	BooleanExpression
}

// StringField is the interface of a generated field of string type.
type StringField interface {
	StringExpression
}

func newFieldExpression(tableName string, fieldName string) expression {
	tableNameSqlArray := quoteIdentifier(tableName)
	fieldNameSqlArray := quoteIdentifier(fieldName)

	var fullFieldNameSqlArray dialectArray
	for dialect := dialect(0); dialect < dialectCount; dialect++ {
		fullFieldNameSqlArray[dialect] = tableNameSqlArray[dialect] + "." + fieldNameSqlArray[dialect]
	}

	return expression{
		builder: func(scope scope) (string, error) {
			dialect := dialectUnknown
			if scope.Database != nil {
				dialect = scope.Database.dialect
			}
			if len(scope.Tables) != 1 || scope.lastJoin != nil || scope.Tables[0].GetName() != tableName {
				return fullFieldNameSqlArray[dialect], nil
			}
			return fieldNameSqlArray[dialect], nil
		},
	}
}

// NewNumberField creates a reference to a number field. It should only be called from generated code.
func NewNumberField(tableName string, fieldName string) NumberField {
	return newFieldExpression(tableName, fieldName)
}

// NewBooleanField creates a reference to a boolean field. It should only be called from generated code.
func NewBooleanField(tableName string, fieldName string) BooleanField {
	return newFieldExpression(tableName, fieldName)
}

// NewStringField creates a reference to a string field. It should only be called from generated code.
func NewStringField(tableName string, fieldName string) StringField {
	return newFieldExpression(tableName, fieldName)
}

type fieldList []Field

func (fields fieldList) GetSQL(scope scope) (string, error) {
	isSingleTable := len(scope.Tables) == 1 && scope.lastJoin == nil
	var sb strings.Builder
	if len(fields) == 0 {
		for i, table := range scope.Tables {
			if i > 0 {
				sb.WriteString(", ")
			}
			actualTable, ok := table.(actualTable)
			if ok {
				if isSingleTable {
					sb.WriteString(actualTable.GetFieldsSQL())
				} else {
					sb.WriteString(actualTable.GetFullFieldsSQL())
				}
			} else {
				sb.WriteByte('*')
			}
		}
	} else {
		fieldsSql, err := commaFields(scope, fields)
		if err != nil {
			return "", err
		}
		sb.WriteString(fieldsSql)
	}
	return sb.String(), nil
}
