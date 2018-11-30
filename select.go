package sqlingo

import (
	"errors"
	"reflect"
	"strconv"
)

type Select interface {
	GetSQL() (string, error)
	FetchFirst(out ...interface{}) (bool, error)
	FetchAll(out interface{}) error
	FetchCursor() (Cursor, error)
	Exists() (bool, error)
	Count() (int, error)
}

type SelectWithFields interface {
	Select
	From(tables ...Table) SelectWithTables
}

type SelectWithTables interface {
	Select
	SelectOrderBy
	Where(conditions ...BooleanExpression) SelectWithWhere
	GroupBy(expressions ...Expression) SelectWithGroupBy
	Limit(limit int) SelectWithLimit
}

type SelectWithWhere interface {
	Select
	SelectOrderBy
	GroupBy(expressions ...Expression) SelectWithGroupBy
}

type SelectWithGroupBy interface {
	Select
	Having(conditions ...BooleanExpression) SelectWithGroupByHaving
	OrderBy(orderBys ...OrderBy) SelectWithOrder
}

type SelectWithGroupByHaving interface {
	SelectWithOrder
	OrderBy(orderBys ...OrderBy) SelectWithOrder
}

type SelectOrderBy interface {
	OrderBy(orderBys ...OrderBy) SelectWithOrder
}

type SelectWithOrder interface {
	Select
	Limit(limit int) SelectWithLimit
}

type SelectWithLimit interface {
	Select
	Offset(offset int) SelectWithOffset
}

type SelectWithOffset interface {
	Select
}

type selectStatus struct {
	scope    scope
	distinct bool
	fields   []Field
	where    BooleanExpression
	orderBys []OrderBy
	groupBys []Expression
	having   BooleanExpression
	limit    *int
	offset   *int
}

func getFields(fields []interface{}) (result []Field) {
	for _, field := range fields {
		fieldCopy := field
		fieldExpression := expression{builder: func(scope scope) (string, error) {
			sql, _, err := getSQLFromWhatever(scope, fieldCopy)
			if err != nil {
				return "", err
			}
			return sql, nil
		}}
		result = append(result, fieldExpression)
	}
	return
}

func (d *database) Select(fields ...interface{}) SelectWithFields {
	return selectStatus{scope: scope{Database: d}, fields: getFields(fields)}
}

func (s selectStatus) From(tables ...Table) SelectWithTables {
	for _, table := range tables {
		s.scope.Tables = append(s.scope.Tables, table)
	}
	return s
}

func (d *database) SelectFrom(tables ...Table) SelectWithTables {
	s := selectStatus{scope: scope{Database: d}}
	for _, table := range tables {
		s.scope.Tables = append(s.scope.Tables, table)
		fields := table.GetFields()
		for _, field := range fields {
			s.fields = append(s.fields, field)
		}
	}
	return s
}

func (d *database) SelectDistinct(fields ...interface{}) SelectWithFields {
	return selectStatus{scope: scope{Database: d}, fields: getFields(fields), distinct: true}
}

func (s selectStatus) Where(conditions ...BooleanExpression) SelectWithWhere {
	s.where = And(conditions...)
	return s
}

func (s selectStatus) GroupBy(expressions ...Expression) SelectWithGroupBy {
	s.groupBys = append([]Expression{}, expressions...)
	return s
}

func (s selectStatus) Having(conditions ...BooleanExpression) SelectWithGroupByHaving {
	s.having = And(conditions...)
	return s
}

func (s selectStatus) OrderBy(orderBys ...OrderBy) SelectWithOrder {
	s.orderBys = append([]OrderBy{}, orderBys...)
	return s
}

func (s selectStatus) Limit(limit int) SelectWithLimit {
	s.limit = &limit
	return s
}

func (s selectStatus) Offset(offset int) SelectWithOffset {
	s.limit = &offset
	return s
}

func (s selectStatus) Count() (count int, err error) {
	if len(s.groupBys) == 0 {
		if s.distinct {
			var fields []interface{}
			for _, field := range s.fields {
				fields = append(fields, field)
			}
			s.distinct = false
			s.fields = []Field{expression{builder: func(scope scope) (string, error) {
				valuesSql, err := commaValues(scope, fields)
				if err != nil {
					return "", err
				}
				return "COUNT(DISTINCT " + valuesSql + ")", nil
			}}}
			_, err = s.FetchFirst(&count)
		} else {
			s.fields = []Field{staticExpression("COUNT(1)", 0)}
			_, err = s.FetchFirst(&count)
		}
	} else {
		_, err = s.scope.Database.Select(Function("COUNT", 1)).From(s.asDerivedTable("t")).FetchFirst(&count)
	}

	return
}

func (s selectStatus) asDerivedTable(name string) Table {
	return derivedTable{
		name:    name,
		select_: s,
	}
}

func (s selectStatus) Exists() (exists bool, err error) {
	_, err = s.scope.Database.Select(command(Raw("EXISTS"), s)).FetchFirst(&exists)
	return
}

func (s selectStatus) GetSQL() (string, error) {
	sql := "SELECT "
	if s.distinct {
		sql += "DISTINCT "
	}

	fieldsSql, err := commaFields(s.scope, s.fields)
	if err != nil {
		return "", err
	}
	sql += fieldsSql

	if len(s.scope.Tables) > 0 {
		var values []interface{}
		for _, table := range s.scope.Tables {
			values = append(values, table)
		}
		fromSql, err := commaValues(s.scope, values)
		if err != nil {
			return "", err
		}
		sql += " FROM " + fromSql
	}

	if s.where != nil {
		whereSql, err := s.where.GetSQL(s.scope)
		if err != nil {
			return "", err
		}
		sql += " WHERE " + whereSql
	}

	if len(s.groupBys) != 0 {
		groupBySql, err := commaExpressions(s.scope, s.groupBys)
		if err != nil {
			return "", err
		}
		sql += " GROUP BY " + groupBySql

		if s.having != nil {
			havingSql, err := s.having.GetSQL(s.scope)
			if err != nil {
				return "", err
			}
			sql += " HAVING " + havingSql
		}
	}

	if len(s.orderBys) > 0 {
		orderBySql, err := commaOrderBys(s.scope, s.orderBys)
		if err != nil {
			return "", err
		}
		sql += " ORDER BY " + orderBySql
	}

	if s.limit != nil {
		sql += " LIMIT " + strconv.Itoa(*s.limit)
	}

	if s.offset != nil {
		sql += " OFFSET " + strconv.Itoa(*s.offset)
	}

	return sql, nil
}

func (s selectStatus) FetchCursor() (Cursor, error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}

	cursor, err := s.scope.Database.Query(sqlString)
	if err != nil {
		return nil, err
	}
	return cursor, nil
}

func (s selectStatus) FetchFirst(dest ...interface{}) (ok bool, err error) {
	cursor, err := s.FetchCursor()
	if err != nil {
		return
	}
	defer cursor.Close()

	for cursor.Next() {
		err = cursor.Scan(dest...)
		if err != nil {
			return
		}
		ok = true
		break
	}

	return
}

func (s selectStatus) FetchAll(dest interface{}) error {
	if reflect.ValueOf(dest).Kind() != reflect.Ptr {
		return errors.New("dest should be a pointer")
	}
	val := reflect.Indirect(reflect.ValueOf(dest))
	if val.Kind() != reflect.Slice {
		return errors.New("dest should be pointed to a slice")
	}
	cursor, err := s.FetchCursor()
	if err != nil {
		return err
	}
	defer cursor.Close()

	for cursor.Next() {
		elem := reflect.New(val.Type().Elem())
		row := elem.Interface()
		err = cursor.Scan(row)
		if err != nil {
			return err
		}
		val.Set(reflect.Append(val, reflect.Indirect(elem)))
	}
	return nil
}
