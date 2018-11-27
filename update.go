package sqlingo

import (
	"database/sql"
)

type updateStatus struct {
	scope       scope
	assignments []assignment
	where       BooleanExpression
}

func (s *updateStatus) copy() *updateStatus {
	update := *s
	update.assignments = append([]assignment{}, s.assignments...)
	return &update
}

func (d *database) Update(table Table) UpdateWithTable {
	return &updateStatus{scope: scope{Database: d, Tables: []Table{table}}}
}

type UpdateWithTable interface {
	Set(field Field, value interface{}) UpdateWithSet
	SetMap(map[Field]interface{}) UpdateWithSet
}

type UpdateWithSet interface {
	Set(Field Field, value interface{}) UpdateWithSet
	Where(conditions ...BooleanExpression) UpdateWithWhere
}

type UpdateWithWhere interface {
	GetSQL() (string, error)
	Execute() (sql.Result, error)
}

func (s *updateStatus) Set(field Field, value interface{}) UpdateWithSet {
	update := s.copy()
	update.assignments = append(update.assignments, assignment{
		field: field,
		value: value,
	})
	return update
}

func (s *updateStatus) SetMap(values map[Field]interface{}) UpdateWithSet {
	update := s.copy()
	for field, value := range values {
		update.assignments = append(update.assignments, assignment{
			field: field,
			value: value,
		})
	}
	return update
}

func (s *updateStatus) Where(conditions ...BooleanExpression) UpdateWithWhere {
	update := s.copy()
	update.where = And(conditions...)
	return update
}

func (s *updateStatus) GetSQL() (string, error) {
	sqlString := "UPDATE " + s.scope.Tables[0].GetSQL(s.scope)

	assignmentsSql, err := commaAssignments(s.scope, s.assignments)
	if err != nil {
		return "", err
	}
	sqlString += " SET " + assignmentsSql

	whereSql, err := s.where.GetSQL(s.scope)
	if err != nil {
		return "", err
	}
	sqlString += " WHERE " + whereSql

	return sqlString, nil
}

func (s *updateStatus) Execute() (sql.Result, error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}
	return s.scope.Database.Execute(sqlString)
}
