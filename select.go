package sqlingo

import (
	"errors"
	"reflect"
	"strconv"
)

type Select interface {
	GetFields() []Field
	GetSQL() string
	FetchFirst(out ...interface{}) error
	FetchAll(out interface{}) error
	FetchCursor() (Cursor, error)
	Exists() (bool, error)
}

type SelectWithFields interface {
	Select
	From(tables ...Table) SelectWithTables
}

type SelectWithTables interface {
	Select
	SelectOrderBy
	Count() (int, error)
	Where(conditions ...BooleanExpression) SelectWithWhere
	GroupBy(expressions ...Expression) SelectWithGroupBy
}

type SelectWithWhere interface {
	Select
	SelectOrderBy
	Count() (int, error)
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
	database *Database
	fields   []Field
	tables   []*Table
	where    *BooleanExpression
	orderBys []OrderBy
	groupBys []Expression
	having   *BooleanExpression
	limit    *int
	offset   *int
	lock     string
}

func (s *selectStatus) copy() *selectStatus {
	select_ := *s
	select_.fields = s.GetFields()
	return &select_
}

func (s *selectStatus) GetFields() []Field {
	var fields []Field
	fields = append(fields, s.fields...)
	return fields
}

func (d *Database) Select(fields ...interface{}) SelectWithFields {
	select_ := &selectStatus{database: d}
	for _, field := range fields {
		sql, priority := getSQLFromWhatever(field)
		expression := &expression{sql: sql, priority: priority}
		select_.fields = append(select_.fields, expression)
	}
	return select_
}

func (s *selectStatus) From(tables ...Table) SelectWithTables {
	select_ := s.copy()
	for _, table := range tables {
		select_.tables = append(select_.tables, &table)
	}
	return select_
}

func (d *Database) SelectFrom(tables ...Table) SelectWithTables {
	select_ := selectStatus{database: d}
	for _, table := range tables {
		select_.tables = append(select_.tables, &table)
		fields := table.GetFields()
		for _, field := range fields {
			select_.fields = append(select_.fields, field)
		}
	}

	return &select_
}

func (s *selectStatus) Where(conditions ...BooleanExpression) SelectWithWhere {
	select_ := s.copy()
	condition := And(conditions...)
	select_.where = &condition
	return select_
}

func (s *selectStatus) GroupBy(expressions ...Expression) SelectWithGroupBy {
	select_ := s.copy()
	select_.groupBys = expressions
	return select_
}

func (s *selectStatus) Having(conditions ...BooleanExpression) SelectWithGroupByHaving {
	select_ := s.copy()
	condition := And(conditions...)
	select_.having = &condition
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
	select_ := s.copy()
	select_.fields = []Field{Function("COUNT", 1)}

	err = select_.FetchFirst(&count)
	return
}

func (s *selectStatus) Exists() (exists bool, err error) {
	err = s.database.Select(Function("EXISTS", s)).FetchFirst(&exists)
	return
}

func (s *selectStatus) GetSQL() string {
	sql := getCallerInfo() + "SELECT " + commaFields(s.fields)

	if len(s.tables) > 0 {
		var values []interface{}
		for _, table := range s.tables {
			values = append(values, table)
		}
		sql += " FROM " + commaValues(values)
	}

	if s.where != nil {
		sql += " WHERE " + (*s.where).GetSQL()
	}

	if len(s.groupBys) != 0 {
		sql += " GROUP BY " + commaExpressions(s.groupBys)

		if s.having != nil {
			sql += " HAVING " + (*s.having).GetSQL()
		}
	}

	if len(s.orderBys) > 0 {
		sql += " ORDER BY " + commaOrderBys(s.orderBys)
	}

	if s.limit != nil {
		sql += " LIMIT " + strconv.Itoa(*s.limit)
	}

	if s.offset != nil {
		sql += " OFFSET " + strconv.Itoa(*s.offset)
	}

	sql += s.lock

	return sql
}

func (s *selectStatus) FetchCursor() (Cursor, error) {
	sqlString := s.GetSQL()

	cursor, err := s.database.Query(sqlString)
	if err != nil {
		return nil, err
	}
	return cursor, nil
}

func (s *selectStatus) FetchFirst(dest ...interface{}) error {
	cursor, err := s.FetchCursor()
	if err != nil {
		return err
	}
	defer cursor.Close()

	for cursor.Next() {
		err = cursor.Scan(dest...)
		if err != nil {
			return err
		}
		break
	}

	return nil
}

func (s *selectStatus) FetchAll(dest interface{}) error {
	if reflect.ValueOf(dest).Kind() != reflect.Ptr {
		return errors.New("dest should be a pointer")
	}
	val := reflect.Indirect(reflect.ValueOf(dest))
	if val.Kind() == reflect.Slice {
		cursor, err := s.FetchCursor()
		if err != nil {
			return err
		}
		defer cursor.Close()

		for cursor.Next() {
			if err != nil {
				return err
			}
			elem := reflect.New(val.Type().Elem())
			row := elem.Interface()
			cursor.Scan(row)
			val.Set(reflect.Append(val, reflect.Indirect(elem)))
		}
		return nil
	}
	return nil

}
