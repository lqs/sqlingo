package sqlingo

import (
	"database/sql"
)

type updateStatus struct {
	database    *Database
	table       *Table
	assignments []assignment
	where       *BooleanExpression
}

func (s *updateStatus) copy() *updateStatus {
	update := *s
	update.assignments = append([]assignment{}, s.assignments...)
	return &update
}

func (d *Database) Update(table Table) UpdateWithTable {
	return &updateStatus{database: d, table: &table}
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
	condition := And(conditions...)
	update.where = &condition
	return update
}

func (s *updateStatus) GetSQL() (string, error) {
	sqlString := getCallerInfo() + "UPDATE " + (*s.table).GetSQL() +
		" SET " + commaAssignments(s.assignments) +
		" WHERE " + (*s.where).GetSQL()

	return sqlString, nil
}

func (s *updateStatus) Execute() (sql.Result, error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}
	return s.database.Execute(sqlString)
}
