package sqlingo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type insertStatus struct {
	method                          string
	scope                           scope
	fields                          []Field
	values                          []interface{}
	models                          []interface{}
	onDuplicateKeyUpdateAssignments []assignment
	ctx                             context.Context
}

type insertWithTable interface {
	Fields(fields ...Field) insertWithValues
	Values(values ...interface{}) insertWithValues
	Models(models ...interface{}) insertWithModels
}

type insertWithValues interface {
	toInsertWithContext
	toInsertFinal
	Values(values ...interface{}) insertWithValues
	OnDuplicateKeyIgnore() toInsertFinal
	OnDuplicateKeyUpdate() insertWithOnDuplicateKeyUpdateBegin
}

type insertWithModels interface {
	toInsertWithContext
	toInsertFinal
	Models(models ...interface{}) insertWithModels
	OnDuplicateKeyIgnore() toInsertFinal
	OnDuplicateKeyUpdate() insertWithOnDuplicateKeyUpdateBegin
}

type insertWithOnDuplicateKeyUpdateBegin interface {
	Set(Field Field, value interface{}) insertWithOnDuplicateKeyUpdate
	SetIf(condition bool, Field Field, value interface{}) insertWithOnDuplicateKeyUpdate
}

type insertWithOnDuplicateKeyUpdate interface {
	toInsertWithContext
	toInsertFinal
	Set(Field Field, value interface{}) insertWithOnDuplicateKeyUpdate
	SetIf(condition bool, Field Field, value interface{}) insertWithOnDuplicateKeyUpdate
}

type toInsertWithContext interface {
	WithContext(ctx context.Context) toInsertFinal
}

type toInsertFinal interface {
	GetSQL() (string, error)
	Execute() (result sql.Result, err error)
}

func (d *database) InsertInto(table Table) insertWithTable {
	return insertStatus{method: "INSERT", scope: scope{Database: d, Tables: []Table{table}}}
}

func (d *database) ReplaceInto(table Table) insertWithTable {
	return insertStatus{method: "REPLACE", scope: scope{Database: d, Tables: []Table{table}}}
}

func (s insertStatus) Fields(fields ...Field) insertWithValues {
	s.fields = fields
	return s
}

func (s insertStatus) Values(values ...interface{}) insertWithValues {
	s.values = append([]interface{}{}, s.values...)
	s.values = append(s.values, values)
	return s
}

func addModel(models *[]Model, model interface{}) error {
	if model, ok := model.(Model); ok {
		*models = append(*models, model)
		return nil
	}

	value := reflect.ValueOf(model)
	switch value.Kind() {
	case reflect.Ptr:
		value = reflect.Indirect(value)
		return addModel(models, value.Interface())
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			elem := value.Index(i)
			addr := elem.Addr()
			inter := addr.Interface()
			if err := addModel(models, inter); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown model type (kind = %d)", value.Kind())
	}
}

func (s insertStatus) Models(models ...interface{}) insertWithModels {
	s.models = models
	return s
}

func (s insertStatus) OnDuplicateKeyUpdate() insertWithOnDuplicateKeyUpdateBegin {
	return s
}

func (s insertStatus) SetIf(condition bool, field Field, value interface{}) insertWithOnDuplicateKeyUpdate {
	if condition {
		return s.Set(field, value)
	}
	return s
}

func (s insertStatus) Set(field Field, value interface{}) insertWithOnDuplicateKeyUpdate {
	s.onDuplicateKeyUpdateAssignments = append([]assignment{}, s.onDuplicateKeyUpdateAssignments...)
	s.onDuplicateKeyUpdateAssignments = append(s.onDuplicateKeyUpdateAssignments, assignment{
		field: field,
		value: value,
	})
	return s
}

func (s insertStatus) OnDuplicateKeyIgnore() toInsertFinal {
	firstField := s.scope.Tables[0].GetFields()[0]
	return s.OnDuplicateKeyUpdate().Set(firstField, firstField)
}

func (s insertStatus) GetSQL() (string, error) {
	var fields []Field
	var valuesBuilder strings.Builder
	if len(s.models) > 0 {
		models := make([]Model, 0, len(s.models))
		for _, model := range s.models {
			if err := addModel(&models, model); err != nil {
				return "", err
			}
		}

		if len(models) > 0 {
			fields = models[0].GetTable().GetFields()
			valuesBuilder.Grow(64 * len(models))
			for i, model := range models {
				if model.GetTable().GetName() != s.scope.Tables[0].GetName() {
					return "", errors.New("invalid table from model")
				}
				if i > 0 {
					valuesBuilder.WriteString(", ")
				}
				valuesBuilder.WriteByte('(')
				if err := commaValuesBuilder(s.scope, &valuesBuilder, model.GetValues()); err != nil {
					return "", err
				}
				valuesBuilder.WriteByte(')')
			}
		}
	} else {
		if len(s.fields) == 0 {
			fields = s.scope.Tables[0].GetFields()
		} else {
			fields = s.fields
		}
		if err := commaValuesBuilder(s.scope, &valuesBuilder, s.values); err != nil {
			return "", err
		}
	}

	if valuesBuilder.Len() == 0 {
		return "/* INSERT without VALUES */ DO 0", nil
	}

	tableSql := s.scope.Tables[0].GetSQL(s.scope)
	fieldsSql, err := commaFields(s.scope, fields)
	if err != nil {
		return "", err
	}

	sqlString := s.method + " INTO " + tableSql + " (" + fieldsSql + ") VALUES " + valuesBuilder.String()
	if len(s.onDuplicateKeyUpdateAssignments) > 0 {
		assignmentsSql, err := commaAssignments(s.scope, s.onDuplicateKeyUpdateAssignments)
		if err != nil {
			return "", err
		}
		sqlString += " ON DUPLICATE KEY UPDATE " + assignmentsSql
	}

	return sqlString, nil
}

func (s insertStatus) WithContext(ctx context.Context) toInsertFinal {
	s.ctx = ctx
	return s
}

func (s insertStatus) Execute() (result sql.Result, err error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}
	return s.scope.Database.ExecuteContext(s.ctx, sqlString)
}
