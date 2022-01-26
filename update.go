package sqlingo

import (
	"database/sql"
	"strconv"
	"strings"
)

type updateStatus struct {
	scope       scope
	assignments []assignment
	where       BooleanExpression
	orderBys    []OrderBy
	limit       *int
}

func (d *database) Update(table Table) updateWithSet {
	return updateStatus{scope: scope{Database: d, Tables: []Table{table}}}
}

type updateWithSet interface {
	Set(Field Field, value interface{}) updateWithSet
	SetIf(prerequisite bool, Field Field, value interface{}) updateWithSet
	Where(conditions ...BooleanExpression) updateWithWhere
	OrderBy(orderBys ...OrderBy) updateWithOrder
	Limit(limit int) updateWithLimit
}

type updateWithWhere interface {
	toUpdateFinal
	OrderBy(orderBys ...OrderBy) updateWithOrder
	Limit(limit int) updateWithLimit
}

type updateWithOrder interface {
	toUpdateFinal
	Limit(limit int) updateWithLimit
}

type updateWithLimit interface {
	toUpdateFinal
}

type toUpdateFinal interface {
	GetSQL() (string, error)
	Execute() (sql.Result, error)
}

func (s updateStatus) Set(field Field, value interface{}) updateWithSet {
	s.assignments = append([]assignment{}, s.assignments...)
	s.assignments = append(s.assignments, assignment{
		field: field,
		value: value,
	})
	return s
}

func (s updateStatus) SetIf(prerequisite bool, field Field, value interface{}) updateWithSet {
	if prerequisite {
		return s.Set(field, value)
	}
	return s
}

func (s updateStatus) Where(conditions ...BooleanExpression) updateWithWhere {
	s.where = And(conditions...)
	return s
}

func (s updateStatus) OrderBy(orderBys ...OrderBy) updateWithOrder {
	s.orderBys = orderBys
	return s
}

func (s updateStatus) Limit(limit int) updateWithLimit {
	s.limit = &limit
	return s
}

func (s updateStatus) GetSQL() (string, error) {
	if len(s.assignments) == 0 {
		return "/* UPDATE without SET clause */ DO 0", nil
	}
	var sb strings.Builder
	sb.Grow(128)

	sb.WriteString("UPDATE ")
	sb.WriteString(s.scope.Tables[0].GetSQL(s.scope))

	assignmentsSql, err := commaAssignments(s.scope, s.assignments)
	if err != nil {
		return "", err
	}
	sb.WriteString(" SET ")
	sb.WriteString(assignmentsSql)

	if s.where != nil {
		whereSql, err := s.where.GetSQL(s.scope)
		if err != nil {
			return "", err
		}
		sb.WriteString(" WHERE ")
		sb.WriteString(whereSql)
	}

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

func (s updateStatus) Execute() (sql.Result, error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}
	return s.scope.Database.Execute(sqlString)
}
