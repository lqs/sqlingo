package sqlingo

import (
	"database/sql"
)

type DeleteWithTable interface {
	Where(conditions ...BooleanExpression) DeleteWithWhere
}

type DeleteWithWhere interface {
	GetSQL() (string, error)
	Execute() (result sql.Result, err error)
}

type deleteStatus struct {
	database Database
	table    *Table
	where    *BooleanExpression
}

func (s *deleteStatus) copy() *deleteStatus {
	delete_ := *s
	return &delete_
}

func (d *database) DeleteFrom(table Table) DeleteWithTable {
	return &deleteStatus{database: d, table: &table}
}

func (s *deleteStatus) Where(conditions ...BooleanExpression) DeleteWithWhere {
	delete_ := s.copy()
	condition := And(conditions...)
	delete_.where = &condition
	return delete_
}

func (s *deleteStatus) GetSQL() (string, error) {
	sqlString := getCallerInfo(s.database) + "DELETE FROM " + (*s.table).GetSQL() + " WHERE " + (*s.where).GetSQL()

	return sqlString, nil
}

func (s *deleteStatus) Execute() (sql.Result, error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}
	return s.database.Execute(sqlString)
}
