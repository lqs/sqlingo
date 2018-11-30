package sqlingo

import (
	"database/sql"
	"reflect"
)

type insertStatus struct {
	scope                           scope
	fields                          []Field
	values                          []interface{}
	models                          []Model
	onDuplicateKeyUpdateAssignments []assignment
}

type InsertWithTable interface {
	Fields(fields ...Field) InsertWithValues
	Values(values ...interface{}) InsertWithValues
	Models(models ...interface{}) InsertWithModels
}

type InsertWithValues interface {
	Values(values ...interface{}) InsertWithValues
	OnDuplicateKeyUpdate() InsertWithOnDuplicateKeyUpdateBegin
	Execute() (result sql.Result, err error)
}

type InsertWithModels interface {
	Models(models ...interface{}) InsertWithModels
	OnDuplicateKeyUpdate() InsertWithOnDuplicateKeyUpdateBegin
	GetSQL() (string, error)
	Execute() (result sql.Result, err error)
}

type InsertWithOnDuplicateKeyUpdateBegin interface {
	Set(Field Field, value interface{}) InsertWithOnDuplicateKeyUpdate
}

type InsertWithOnDuplicateKeyUpdate interface {
	Set(Field Field, value interface{}) InsertWithOnDuplicateKeyUpdate
	GetSQL() (string, error)
	Execute() (result sql.Result, err error)
}

func (d *database) InsertInto(table Table) InsertWithTable {
	return insertStatus{scope: scope{Database: d, Tables: []Table{table}}}
}

func (s insertStatus) Fields(fields ...Field) InsertWithValues {
	s.fields = fields
	return s
}

func (s insertStatus) Values(values ...interface{}) InsertWithValues {
	s.values = append([]interface{}{}, s.values...)
	s.values = append(s.values, values...)
	return s
}

func (s *insertStatus) addModel(model interface{}) {
	m0, ok := model.(Model)
	if ok {
		s.models = append(s.models, m0)
		return
	}

	value := reflect.ValueOf(model)
	if value.Kind() == reflect.Ptr {
		value = reflect.Indirect(value)
		s.addModel(value.Interface())
	}
	if value.Kind() == reflect.Slice {
		for i := 0; i < value.Len(); i++ {
			elem := value.Index(i)
			addr := elem.Addr()
			inter := addr.Interface()
			s.addModel(inter)
		}
		return
	}
}

func (s insertStatus) Models(models ...interface{}) InsertWithModels {
	if len(models) == 0 {
		return s
	}

	for _, model := range models {
		s.addModel(model)
	}
	return s
}

func (s insertStatus) OnDuplicateKeyUpdate() InsertWithOnDuplicateKeyUpdateBegin {
	return s
}

func (s insertStatus) Set(field Field, value interface{}) InsertWithOnDuplicateKeyUpdate {
	s.onDuplicateKeyUpdateAssignments = append([]assignment{}, s.onDuplicateKeyUpdateAssignments...)
	s.onDuplicateKeyUpdateAssignments = append(s.onDuplicateKeyUpdateAssignments, assignment{
		field: field,
		value: value,
	})
	return s
}

func (s insertStatus) GetSQL() (string, error) {
	var fields []Field
	var values []interface{}
	if len(s.models) > 0 {
		fields = s.models[0].GetTable().GetFields()
		for _, model := range s.models {
			values = append(values, model.GetValues())
		}
	} else {
		fields = s.fields
		values = s.values
	}

	tableSql := s.scope.Tables[0].GetSQL(s.scope)
	fieldsSql, err := commaFields(s.scope, fields)
	if err != nil {
		return "", err
	}
	valuesSql, err := commaValues(s.scope, values)
	if err != nil {
		return "", err
	}

	sqlString := "INSERT INTO " + tableSql + " (" + fieldsSql + ") VALUES " + valuesSql
	if len(s.onDuplicateKeyUpdateAssignments) > 0 {
		assignmentsSql, err := commaAssignments(s.scope, s.onDuplicateKeyUpdateAssignments)
		if err != nil {
			return "", err
		}
		sqlString += " ON DUPLICATE KEY UPDATE " + assignmentsSql
	}

	return sqlString, nil

}

func (s insertStatus) Execute() (result sql.Result, err error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}
	return s.scope.Database.Execute(sqlString)
}
