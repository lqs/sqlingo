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

func (s *selectStatus) copy() *selectStatus {
	select_ := *s
	return &select_
}

func (d *database) Select(fields ...interface{}) SelectWithFields {
	select_ := &selectStatus{scope: scope{Database: d}}
	for _, field := range fields {
		fieldCopy := field
		fieldExpression := expression{builder: func(scope scope) (string, error) {
			sql, _, err := getSQLFromWhatever(scope, fieldCopy)
			if err != nil {
				return "", err
			}
			return sql, nil
		}}
		select_.fields = append(select_.fields, fieldExpression)
	}
	return select_
}

func (s *selectStatus) From(tables ...Table) SelectWithTables {
	select_ := s.copy()
	for _, table := range tables {
		select_.scope.Tables = append(select_.scope.Tables, table)
	}
	return select_
}

func (d *database) SelectFrom(tables ...Table) SelectWithTables {
	select_ := selectStatus{scope: scope{Database: d}}
	for _, table := range tables {
		select_.scope.Tables = append(select_.scope.Tables, table)
		fields := table.GetFields()
		for _, field := range fields {
			select_.fields = append(select_.fields, field)
		}
	}

	return &select_
}

func (d *database) SelectDistinct(fields ...interface{}) SelectWithFields {
	select_ := d.Select(fields...)
	select_.(*selectStatus).distinct = true
	return select_
}

func (s *selectStatus) Where(conditions ...BooleanExpression) SelectWithWhere {
	select_ := s.copy()
	select_.where = And(conditions...)
	return select_
}

func (s *selectStatus) GroupBy(expressions ...Expression) SelectWithGroupBy {
	select_ := s.copy()
	select_.groupBys = expressions
	return select_
}

func (s *selectStatus) Having(conditions ...BooleanExpression) SelectWithGroupByHaving {
	select_ := s.copy()
	select_.having = And(conditions...)
	return select_
}

func (s *selectStatus) OrderBy(orderBys ...OrderBy) SelectWithOrder {
	select_ := s.copy()
	select_.orderBys = append(select_.orderBys, orderBys...)
	return select_
}

func (s *selectStatus) Limit(limit int) SelectWithLimit {
	select_ := s.copy()
	select_.limit = &limit
	return select_
}

func (s *selectStatus) Offset(offset int) SelectWithOffset {
	select_ := s.copy()
	select_.limit = &offset
	return select_
}

func (s *selectStatus) Count() (count int, err error) {
	if len(s.groupBys) == 0 {
		if s.distinct {
			select_ := s.copy()
			var fields []interface{}
			for _, field := range select_.fields {
				fields = append(fields, field)
			}
			select_.distinct = false
			select_.fields = []Field{expression{builder: func(scope scope) (string, error) {
				valuesSql, err := commaValues(scope, fields)
				if err != nil {
					return "", err
				}
				return "COUNT(DISTINCT " + valuesSql + ")", nil
			}}}
			_, err = select_.FetchFirst(&count)
		} else {
			select_ := s.copy()
			select_.fields = []Field{staticExpression("COUNT(1)", 0)}
			_, err = select_.FetchFirst(&count)
		}
	} else {
		_, err = s.scope.Database.Select(Function("COUNT", 1)).From(s.asDerivedTable("t")).FetchFirst(&count)
	}

	return
}

func (s *selectStatus) asDerivedTable(name string) Table {
	return &derivedTable{
		name:    name,
		select_: *s,
	}
}

func (s *selectStatus) Exists() (exists bool, err error) {
	_, err = s.scope.Database.Select(command(Raw("EXISTS"), s)).FetchFirst(&exists)
	return
}

func (s *selectStatus) GetSQL() (string, error) {
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

func (s *selectStatus) FetchCursor() (Cursor, error) {
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

func (s *selectStatus) FetchFirst(dest ...interface{}) (ok bool, err error) {
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

func (s *selectStatus) FetchAll(dest interface{}) error {
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
