package sqlingo

import "fmt"

// WellKnownBinaryField is the interface of a generated field of binary geometry (WKB) type.
type WellKnownBinaryField interface {
	WellKnownBinaryExpression
	GetTable() Table
}

// WellKnownBinaryExpression is the interface of an SQL expression with binary geometry (WKB) value.
type WellKnownBinaryExpression interface {
	Expression
	STAsText() StringExpression
}

// WellKnownBinary is the type of geometry well-known binary (WKB) field.
type WellKnownBinary []byte

// NewWellKnownBinaryField creates a reference to a geometry WKB field. It should only be called from generated code.
func NewWellKnownBinaryField(table Table, fieldName string) WellKnownBinaryField {
	return newField(table, fieldName)
}

func (e expression) STAsText() StringExpression {
	return function("ST_AsText", e)
}

func STGeomFromText(text interface{}) WellKnownBinaryExpression {
	return function("ST_GeomFromText", text)
}

func STGeomFromTextf(format string, a ...interface{}) WellKnownBinaryExpression {
	text := fmt.Sprintf(format, a...)
	return STGeomFromText(text)
}
