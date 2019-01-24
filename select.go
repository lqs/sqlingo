package sqlingo

import (
	"context"
	"errors"
	"reflect"
	"strconv"
)

type Select interface {
	GetSQL() (string, error)
	FetchFirst(out ...interface{}) (bool, error)
	FetchAll(dest ...interface{}) (rows int, err error)
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
	OrderBy(orderBys ...OrderBy) SelectWithOrder
	Where(conditions ...BooleanExpression) SelectWithWhere
	GroupBy(expressions ...Expression) SelectWithGroupBy
	Limit(limit int) SelectWithLimit
}

type SelectWithWhere interface {
	Select
	GroupBy(expressions ...Expression) SelectWithGroupBy
	OrderBy(orderBys ...OrderBy) SelectWithOrder
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

type SelectWithContext interface {
	FetchFirst(out ...interface{}) (bool, error)
	FetchAll(dest ...interface{}) (rows int, err error)
	FetchCursor() (Cursor, error)
	Exists() (bool, error)
	Count() (int, error)
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
	ctx      context.Context
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
	if len(s.fields) == 0 {
		return s.scope.Database.SelectFrom(tables...)
	}
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

func (s selectStatus) WithContext(ctx context.Context) SelectWithContext {
	s.ctx = ctx
	return s
}

func (s selectStatus) getContext() context.Context {
	if s.ctx != nil {
		return s.ctx
	} else {
		return context.Background()
	}
}

func (s selectStatus) FetchCursor() (Cursor, error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}

	cursor, err := s.scope.Database.QueryContext(s.getContext(), sqlString)
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

func (s selectStatus) fetchAllAsMap(cursor Cursor, mapType reflect.Type) (mapValue reflect.Value, err error) {
	mapValue = reflect.MakeMap(mapType)
	key := reflect.New(mapType.Key())
	elem := reflect.New(mapType.Elem())

	for cursor.Next() {
		err = cursor.Scan(key.Interface(), elem.Interface())
		if err != nil {
			return
		}

		mapValue.SetMapIndex(reflect.Indirect(key), reflect.Indirect(elem))
	}
	return
}

func (s selectStatus) FetchAll(dest ...interface{}) (rows int, err error) {
	cursor, err := s.FetchCursor()
	if err != nil {
		return
	}
	defer cursor.Close()

	count := len(dest)
	values := make([]reflect.Value, count)
	for i, item := range dest {
		if reflect.ValueOf(item).Kind() != reflect.Ptr {
			err = errors.New("dest should be a pointer")
			return
		}
		val := reflect.Indirect(reflect.ValueOf(item))

		switch val.Kind() {
		case reflect.Slice:
			values[i] = val
		case reflect.Map:
			if len(dest) != 1 {
				err = errors.New("dest map should be 1 element")
				return
			}
			var mapValue reflect.Value
			mapValue, err = s.fetchAllAsMap(cursor, val.Type())
			if err != nil {
				return
			}
			reflect.ValueOf(item).Elem().Set(mapValue)
			return
		default:
			err = errors.New("dest should be pointed to a slice")
			return
		}
	}

	elements := make([]reflect.Value, count)
	pointers := make([]interface{}, count)
	for i := 0; i < count; i++ {
		elements[i] = reflect.New(values[i].Type().Elem())
		pointers[i] = elements[i].Interface()
	}
	for cursor.Next() {
		err = cursor.Scan(pointers...)
		if err != nil {
			return
		}
		for i := 0; i < count; i++ {
			values[i].Set(reflect.Append(values[i], reflect.Indirect(elements[i])))
		}
		rows++
	}
	return
}
