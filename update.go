package sqlingo

import (
	"database/sql"
	"errors"
)

type updateStatus struct {
	scope       scope
	assignments []assignment
	where       BooleanExpression
}

func (d *database) Update(table Table) UpdateWithSet {
	return updateStatus{scope: scope{Database: d, Tables: []Table{table}}}
}

type UpdateWithSet interface {
	Set(Field Field, value interface{}) UpdateWithSet
	Where(conditions ...BooleanExpression) UpdateWithWhere
}

type UpdateWithWhere interface {
	GetSQL() (string, error)
	Execute() (sql.Result, error)
}

func (s updateStatus) Set(field Field, value interface{}) UpdateWithSet {
	s.assignments = append([]assignment{}, s.assignments...)
	s.assignments = append(s.assignments, assignment{
		field: field,
		value: value,
	})
	return s
}

func (s updateStatus) Where(conditions ...BooleanExpression) UpdateWithWhere {
	s.where = And(conditions...)
	return s
}

func (s updateStatus) GetSQL() (string, error) {
	sqlString := "UPDATE " + s.scope.Tables[0].GetSQL(s.scope)

	if len(s.assignments) == 0 {
		return "", errors.New("no set in update")
	}
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

func (s updateStatus) Execute() (sql.Result, error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}
	return s.scope.Database.Execute(sqlString)
}
