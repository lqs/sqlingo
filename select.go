package sqlingo

import (
	. "context"
	"errors"
	"reflect"
	"strconv"
	"strings"
)

type SelectWithFields interface {
	toSelectWithContext
	toSelectFinal
	From(tables ...Table) SelectWithTables
}

type SelectWithTables interface {
	toSelectJoin
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	Where(conditions ...BooleanExpression) SelectWithWhere
	GroupBy(expressions ...Expression) SelectWithGroupBy
	OrderBy(orderBys ...OrderBy) SelectWithOrder
	Limit(limit int) SelectWithLimit
}

type toSelectJoin interface {
	Join(table Table) SelectWithJoin
	LeftJoin(table Table) SelectWithJoin
	RightJoin(table Table) SelectWithJoin
}

type SelectWithJoin interface {
	On(condition BooleanExpression) SelectWithJoinOn
}

type SelectWithJoinOn interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	Where(conditions ...BooleanExpression) SelectWithWhere
	GroupBy(expressions ...Expression) SelectWithGroupBy
	OrderBy(orderBys ...OrderBy) SelectWithOrder
	Limit(limit int) SelectWithLimit
}

type SelectWithWhere interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	GroupBy(expressions ...Expression) SelectWithGroupBy
	OrderBy(orderBys ...OrderBy) SelectWithOrder
	Limit(limit int) SelectWithLimit
}

type SelectWithGroupBy interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	Having(conditions ...BooleanExpression) SelectWithGroupByHaving
	OrderBy(orderBys ...OrderBy) SelectWithOrder
	Limit(limit int) SelectWithLimit
}

type SelectWithGroupByHaving interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	OrderBy(orderBys ...OrderBy) SelectWithOrder
}

type SelectWithOrder interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	Limit(limit int) SelectWithLimit
}

type SelectWithLimit interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	Offset(offset int) SelectWithOffset
}

type SelectWithOffset interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
}

type toSelectWithLock interface {
	LockInShareMode() SelectWithLock
	ForUpdate() SelectWithLock
}

type SelectWithLock interface {
	toSelectWithContext
	toSelectFinal
}

type toSelectWithContext interface {
	WithContext(ctx Context) toSelectFinal
}

type toSelectFinal interface {
	Exists() (bool, error)
	Count() (int, error)
	GetSQL() (string, error)
	FetchFirst(out ...interface{}) (bool, error)
	FetchAll(dest ...interface{}) (rows int, err error)
	FetchCursor() (Cursor, error)
}

type join struct {
	previous *join
	prefix   string
	table    Table
	on       BooleanExpression
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
	ctx      Context
	lock     string
}

func (s selectStatus) Join(table Table) SelectWithJoin {
	s.scope.lastJoin = &join{previous: s.scope.lastJoin, table: table}
	return s
}

func (s selectStatus) LeftJoin(table Table) SelectWithJoin {
	s.scope.lastJoin = &join{previous: s.scope.lastJoin, prefix: "LEFT ", table: table}
	return s
}

func (s selectStatus) RightJoin(table Table) SelectWithJoin {
	s.scope.lastJoin = &join{previous: s.scope.lastJoin, prefix: "RIGHT ", table: table}
	return s
}

func (s selectStatus) On(condition BooleanExpression) SelectWithJoinOn {
	join := *s.scope.lastJoin
	join.on = condition
	s.scope.lastJoin = &join
	return s
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
	s.scope.Tables = tables
	return s
}

func (d *database) SelectFrom(tables ...Table) SelectWithTables {
	s := selectStatus{scope: scope{Database: d, Tables: tables}}
	fieldCount := 0
	for _, table := range tables {
		fields := table.GetFields()
		fieldCount += len(fields)
	}
	s.fields = make([]Field, 0, fieldCount)
	for _, table := range tables {
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
	s.groupBys = expressions
	return s
}

func (s selectStatus) Having(conditions ...BooleanExpression) SelectWithGroupByHaving {
	s.having = And(conditions...)
	return s
}

func (s selectStatus) OrderBy(orderBys ...OrderBy) SelectWithOrder {
	s.orderBys = orderBys
	return s
}

func (s selectStatus) Limit(limit int) SelectWithLimit {
	s.limit = &limit
	return s
}

func (s selectStatus) Offset(offset int) SelectWithOffset {
	s.offset = &offset
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

func (s selectStatus) LockInShareMode() SelectWithLock {
	s.lock = " LOCK IN SHARE MODE"
	return s
}

func (s selectStatus) ForUpdate() SelectWithLock {
	s.lock = " FOR UPDATE"
	return s
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
	var sb strings.Builder
	sb.Grow(128)
	sb.WriteString("SELECT ")
	if s.distinct {
		sb.WriteString("DISTINCT ")
	}

	fieldsSql, err := commaFields(s.scope, s.fields)
	if err != nil {
		return "", err
	}
	sb.WriteString(fieldsSql)

	if len(s.scope.Tables) > 0 {
		var values []interface{}
		for _, table := range s.scope.Tables {
			values = append(values, table)
		}
		fromSql, err := commaValues(s.scope, values)
		if err != nil {
			return "", err
		}
		sb.WriteString(" FROM ")
		sb.WriteString(fromSql)
	}

	if s.scope.lastJoin != nil {
		var joins []*join
		for j := s.scope.lastJoin; j != nil; j = j.previous {
			joins = append(joins, j)
		}
		count := len(joins)
		for i := count - 1; i >= 0; i-- {
			join := joins[i]
			onSql, err := join.on.GetSQL(s.scope)
			if err != nil {
				return "", err
			}
			sb.WriteString(join.prefix)
			sb.WriteString(" JOIN ")
			sb.WriteString(join.table.GetSQL(s.scope))
			sb.WriteString(" ON ")
			sb.WriteString(onSql)
		}
	}

	if s.where != nil {
		whereSql, err := s.where.GetSQL(s.scope)
		if err != nil {
			return "", err
		}
		sb.WriteString(" WHERE ")
		sb.WriteString(whereSql)
	}

	if len(s.groupBys) != 0 {
		groupBySql, err := commaExpressions(s.scope, s.groupBys)
		if err != nil {
			return "", err
		}
		sb.WriteString(" GROUP BY ")
		sb.WriteString(groupBySql)

		if s.having != nil {
			havingSql, err := s.having.GetSQL(s.scope)
			if err != nil {
				return "", err
			}
			sb.WriteString(" HAVING ")
			sb.WriteString(havingSql)
		}
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

	if s.offset != nil {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(*s.offset))
	}

	sb.WriteString(s.lock)

	return sb.String(), nil
}

func (s selectStatus) WithContext(ctx Context) toSelectFinal {
	s.ctx = ctx
	return s
}

func (s selectStatus) getContext() Context {
	if s.ctx != nil {
		return s.ctx
	} else {
		return Background()
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
