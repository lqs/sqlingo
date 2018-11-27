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

func (s *insertStatus) copy() *insertStatus {
	insert := *s
	s.fields = append([]Field{}, s.fields...)
	s.values = append([]interface{}{}, s.values...)
	s.onDuplicateKeyUpdateAssignments = append([]assignment{}, s.onDuplicateKeyUpdateAssignments...)
	return &insert
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
	return &insertStatus{scope: scope{Database: d, Tables: []Table{table}}}
}

func (s *insertStatus) Fields(fields ...Field) InsertWithValues {
	insert := s.copy()
	insert.fields = fields
	return insert
}

func (s *insertStatus) Values(values ...interface{}) InsertWithValues {
	insert := s.copy()
	insert.values = append(insert.values, values)
	return insert
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

func (s *insertStatus) Models(models ...interface{}) InsertWithModels {
	if len(models) == 0 {
		return s
	}

	insert := s.copy()
	for _, model := range models {
		insert.addModel(model)
	}
	return insert
}

func (s *insertStatus) OnDuplicateKeyUpdate() InsertWithOnDuplicateKeyUpdateBegin {
	insert := s.copy()
	return insert
}

func (s *insertStatus) Set(field Field, value interface{}) InsertWithOnDuplicateKeyUpdate {
	insert := s.copy()
	insert.onDuplicateKeyUpdateAssignments = append(insert.onDuplicateKeyUpdateAssignments, assignment{
		field: field,
		value: value,
	})
	return insert
}

func (s *insertStatus) GetSQL() (string, error) {
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

func (s *insertStatus) Execute() (result sql.Result, err error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}
	return s.scope.Database.Execute(sqlString)
}
