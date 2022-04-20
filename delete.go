package sqlingo

import (
	"database/sql"
	"strconv"
	"strings"
)

type deleteStatus struct {
	scope    scope
	where    BooleanExpression
	orderBys []OrderBy
	limit    *int
}

type deleteWithTable interface {
	Where(conditions ...BooleanExpression) deleteWithWhere
}

type deleteWithWhere interface {
	toDeleteFinal
	OrderBy(orderBys ...OrderBy) deleteWithOrder
	Limit(limit int) deleteWithLimit
}

type deleteWithOrder interface {
	toDeleteFinal
	Limit(limit int) deleteWithLimit
}

type deleteWithLimit interface {
	toDeleteFinal
}

type toDeleteFinal interface {
	GetSQL() (string, error)
	Execute() (result sql.Result, err error)
}

func (d *database) DeleteFrom(table Table) deleteWithTable {
	return deleteStatus{scope: scope{Database: d, Tables: []Table{table}}}
}

func (s deleteStatus) Where(conditions ...BooleanExpression) deleteWithWhere {
	s.where = And(conditions...)
	return s
}

func (s deleteStatus) OrderBy(orderBys ...OrderBy) deleteWithOrder {
	s.orderBys = orderBys
	return s
}

func (s deleteStatus) Limit(limit int) deleteWithLimit {
	s.limit = &limit
	return s
}

func (s deleteStatus) GetSQL() (string, error) {
	var sb strings.Builder
	sb.Grow(128)

	sb.WriteString("DELETE FROM ")
	sb.WriteString(s.scope.Tables[0].GetSQL(s.scope))
	sb.WriteString(" WHERE ")
	whereSql, err := s.where.GetSQL(s.scope)
	if err != nil {
		return "", err
	}
	sb.WriteString(whereSql)

	if len(s.orderBys) > 0 {
		orderBySql, err := commaOrderBys(s.scope, s.orderBys)
		if err != nil {
			return "", err
		}
		sb.WriteString(" ORDER BY ")
		sb.WriteString(orderBySql)
	}

	if s.limit != nil {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.Itoa(*s.limit))
	}

	return sb.String(), nil
}

func (s deleteStatus) Execute() (sql.Result, error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}
	return s.scope.Database.Execute(sqlString)
}
