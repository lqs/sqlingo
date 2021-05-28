package sqlingo

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
)

type selectWithFields interface {
	toSelectWithContext
	toSelectFinal
	From(tables ...Table) selectWithTables
}

type selectWithTables interface {
	toSelectJoin
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	toUnionSelect
	Where(conditions ...BooleanExpression) selectWithWhere
	GroupBy(expressions ...Expression) selectWithGroupBy
	OrderBy(orderBys ...OrderBy) selectWithOrder
	Limit(limit int) selectWithLimit
}

type toSelectJoin interface {
	Join(table Table) selectWithJoin
	LeftJoin(table Table) selectWithJoin
	RightJoin(table Table) selectWithJoin
}

type selectWithJoin interface {
	On(condition BooleanExpression) selectWithJoinOn
}

type selectWithJoinOn interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	toUnionSelect
	Where(conditions ...BooleanExpression) selectWithWhere
	GroupBy(expressions ...Expression) selectWithGroupBy
	OrderBy(orderBys ...OrderBy) selectWithOrder
	Limit(limit int) selectWithLimit
}

type selectWithWhere interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	toUnionSelect
	GroupBy(expressions ...Expression) selectWithGroupBy
	OrderBy(orderBys ...OrderBy) selectWithOrder
	Limit(limit int) selectWithLimit
}

type selectWithGroupBy interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	toUnionSelect
	Having(conditions ...BooleanExpression) selectWithGroupByHaving
	OrderBy(orderBys ...OrderBy) selectWithOrder
	Limit(limit int) selectWithLimit
}

type selectWithGroupByHaving interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	toUnionSelect
	OrderBy(orderBys ...OrderBy) selectWithOrder
}

type selectWithOrder interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	Limit(limit int) selectWithLimit
}

type selectWithLimit interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
	Offset(offset int) selectWithOffset
}

type selectWithOffset interface {
	toSelectWithLock
	toSelectWithContext
	toSelectFinal
}

type toSelectWithLock interface {
	LockInShareMode() selectWithLock
	ForUpdate() selectWithLock
}

type selectWithLock interface {
	toSelectWithContext
	toSelectFinal
}

type toSelectWithContext interface {
	WithContext(ctx context.Context) toSelectFinal
}

type toUnionSelect interface {
	UnionSelect(fields ...interface{}) selectWithFields
	UnionSelectFrom(tables ...Table) selectWithTables
	UnionSelectDistinct(fields ...interface{}) selectWithFields
	UnionAllSelect(fields ...interface{}) selectWithFields
	UnionAllSelectFrom(tables ...Table) selectWithTables
	UnionAllSelectDistinct(fields ...interface{}) selectWithFields
}

type toSelectFinal interface {
	Exists() (bool, error)
	Count() (int, error)
	GetSQL() (string, error)
	FetchFirst(out ...interface{}) (bool, error)
	FetchExactlyOne(out ...interface{}) error
	FetchAll(dest ...interface{}) (rows int, err error)
	FetchCursor() (Cursor, error)
}

type join struct {
	previous *join
	prefix   string
	table    Table
	on       BooleanExpression
}

type selectBase struct {
	scope    scope
	distinct bool
	fields   fieldList
	where    BooleanExpression
	groupBys []Expression
	having   BooleanExpression
}

type selectStatus struct {
	base      selectBase
	orderBys  []OrderBy
	lastUnion *unionSelectStatus
	limit     *int
	offset    int
	ctx       context.Context
	lock      string
}

type unionSelectStatus struct {
	base     selectBase
	all      bool
	previous *unionSelectStatus
}

func (s *selectStatus) activeSelectBase() *selectBase {
	if s.lastUnion != nil {
		return &s.lastUnion.base
	}
	return &s.base
}

func (s selectStatus) Join(table Table) selectWithJoin {
	return s.join("", table)
}

func (s selectStatus) LeftJoin(table Table) selectWithJoin {
	return s.join("LEFT ", table)
}

func (s selectStatus) RightJoin(table Table) selectWithJoin {
	return s.join("RIGHT ", table)
}

func (s selectStatus) join(prefix string, table Table) selectWithJoin {
	base := s.activeSelectBase()
	base.scope.lastJoin = &join{
		previous: base.scope.lastJoin,
		prefix:   prefix,
		table:    table,
	}
	return s
}

func (s selectStatus) On(condition BooleanExpression) selectWithJoinOn {
	base := s.activeSelectBase()
	join := *base.scope.lastJoin
	join.on = condition
	base.scope.lastJoin = &join
	return s
}

func getFields(fields []interface{}) (result []Field) {
	fields = expandSliceValues(fields)
	result = make([]Field, 0, len(fields))
	for _, field := range fields {
		switch field.(type) {
		case Field:
			result = append(result, field.(Field))
		case Table:
			result = append(result, field.(Table).GetFields()...)
		default:
			fieldCopy := field
			fieldExpression := expression{builder: func(scope scope) (string, error) {
				sql, _, err := getSQL(scope, fieldCopy)
				if err != nil {
					return "", err
				}
				return sql, nil
			}}
			result = append(result, fieldExpression)
		}
	}
	return
}

func (d *database) Select(fields ...interface{}) selectWithFields {
	return selectStatus{
		base: selectBase{
			scope: scope{
				Database: d,
			},
			fields: getFields(fields),
		},
	}
}

func (s selectStatus) From(tables ...Table) selectWithTables {
	s.activeSelectBase().scope.Tables = tables
	return s
}

func (d *database) SelectFrom(tables ...Table) selectWithTables {
	return selectStatus{
		base: selectBase{
			scope: scope{
				Database: d,
				Tables:   tables,
			},
		},
	}
}

func (d *database) SelectDistinct(fields ...interface{}) selectWithFields {
	return selectStatus{
		base: selectBase{
			scope: scope{
				Database: d,
			},
			fields:   getFields(fields),
			distinct: true,
		},
	}
}

func (s selectStatus) Where(conditions ...BooleanExpression) selectWithWhere {
	s.activeSelectBase().where = And(conditions...)
	return s
}

func (s selectStatus) GroupBy(expressions ...Expression) selectWithGroupBy {
	s.activeSelectBase().groupBys = expressions
	return s
}

func (s selectStatus) Having(conditions ...BooleanExpression) selectWithGroupByHaving {
	s.activeSelectBase().having = And(conditions...)
	return s
}

func (s selectStatus) UnionSelect(fields ...interface{}) selectWithFields {
	return s.withUnionSelect(false, false, fields, nil)
}

func (s selectStatus) UnionSelectFrom(tables ...Table) selectWithTables {
	return s.withUnionSelect(false, false, nil, tables)
}

func (s selectStatus) UnionSelectDistinct(fields ...interface{}) selectWithFields {
	return s.withUnionSelect(false, true, fields, nil)
}

func (s selectStatus) UnionAllSelect(fields ...interface{}) selectWithFields {
	return s.withUnionSelect(true, false, fields, nil)
}

func (s selectStatus) UnionAllSelectFrom(tables ...Table) selectWithTables {
	return s.withUnionSelect(true, false, nil, tables)
}

func (s selectStatus) UnionAllSelectDistinct(fields ...interface{}) selectWithFields {
	return s.withUnionSelect(true, true, fields, nil)
}

func (s selectStatus) withUnionSelect(all bool, distinct bool, fields []interface{}, tables []Table) selectStatus {
	s.lastUnion = &unionSelectStatus{
		base: selectBase{
			scope: scope{
				Database: s.base.scope.Database,
				Tables:   tables,
			},
			distinct: distinct,
			fields:   getFields(fields),
		},
		all:      all,
		previous: s.lastUnion,
	}
	return s
}

func (s selectStatus) OrderBy(orderBys ...OrderBy) selectWithOrder {
	s.orderBys = orderBys
	return s
}

func (s selectStatus) Limit(limit int) selectWithLimit {
	s.limit = &limit
	return s
}

func (s selectStatus) Offset(offset int) selectWithOffset {
	s.offset = offset
	return s
}

func (s selectStatus) Count() (count int, err error) {
	if s.lastUnion == nil && len(s.base.groupBys) == 0 && s.limit == nil {
		if s.base.distinct {
			fields := s.base.fields
			s.base.distinct = false
			s.base.fields = []Field{expression{builder: func(scope scope) (string, error) {
				fieldsSql, err := fields.GetSQL(scope)
				if err != nil {
					return "", err
				}
				return "COUNT(DISTINCT " + fieldsSql + ")", nil
			}}}
			_, err = s.FetchFirst(&count)
		} else {
			s.base.fields = []Field{staticExpression("COUNT(1)", 0)}
			_, err = s.FetchFirst(&count)
		}
	} else {
		if !s.base.distinct {
			s.base.fields = []Field{staticExpression("1", 0)}
		}
		_, err = s.base.scope.Database.Select(Function("COUNT", 1)).
			From(s.asDerivedTable("t")).
			FetchFirst(&count)
	}

	return
}

func (s selectStatus) LockInShareMode() selectWithLock {
	s.lock = " LOCK IN SHARE MODE"
	return s
}

func (s selectStatus) ForUpdate() selectWithLock {
	s.lock = " FOR UPDATE"
	return s
}

func (s selectStatus) asDerivedTable(name string) Table {
	return derivedTable{
		name:         name,
		selectStatus: s,
	}
}

func (s selectStatus) Exists() (exists bool, err error) {
	_, err = s.base.scope.Database.Select(command("EXISTS", s)).FetchFirst(&exists)
	return
}

func (s selectBase) buildSelectBase(sb *strings.Builder) error {
	sb.WriteString("SELECT ")
	if s.distinct {
		sb.WriteString("DISTINCT ")
	}

	// find tables from fields if "From" is not specified
	if len(s.scope.Tables) == 0 && len(s.fields) > 0 {
		tableNames := make([]string, 0, len(s.fields))
		tableMap := make(map[string]Table)
		for _, field := range s.fields {
			table := field.GetTable()
			if table == nil {
				continue
			}
			tableName := table.GetName()
			if _, ok := tableMap[tableName]; !ok {
				tableMap[tableName] = table
				tableNames = append(tableNames, tableName)
			}
		}
		for _, tableName := range tableNames {
			table := tableMap[tableName]
			s.scope.Tables = append(s.scope.Tables, table)
		}
	}

	fieldsSql, err := s.fields.GetSQL(s.scope)
	if err != nil {
		return err
	}
	sb.WriteString(fieldsSql)

	if len(s.scope.Tables) > 0 {
		fromSql := commaTables(s.scope, s.scope.Tables)
		sb.WriteString(" FROM ")
		sb.WriteString(fromSql)
	}

	if s.scope.lastJoin != nil {
		var joins []*join
		for j := s.scope.lastJoin; j != nil; j = j.previous {
			joins = append(joins, j)
		}
		for i := len(joins) - 1; i >= 0; i-- {
			join := joins[i]
			onSql, err := join.on.GetSQL(s.scope)
			if err != nil {
				return err
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
			return err
		}
		sb.WriteString(" WHERE ")
		sb.WriteString(whereSql)
	}

	if len(s.groupBys) != 0 {
		groupBySql, err := commaExpressions(s.scope, s.groupBys)
		if err != nil {
			return err
		}
		sb.WriteString(" GROUP BY ")
		sb.WriteString(groupBySql)

		if s.having != nil {
			havingSql, err := s.having.GetSQL(s.scope)
			if err != nil {
				return err
			}
			sb.WriteString(" HAVING ")
			sb.WriteString(havingSql)
		}
	}

	return nil
}

func (s selectStatus) GetSQL() (string, error) {
	var sb strings.Builder
	sb.Grow(128)

	if err := s.base.buildSelectBase(&sb); err != nil {
		return "", err
	}

	var unions []*unionSelectStatus
	for union := s.lastUnion; union != nil; union = union.previous {
		unions = append(unions, union)
	}
	for i := len(unions) - 1; i >= 0; i-- {
		union := unions[i]
		if union.all {
			sb.WriteString(" UNION ALL ")
		} else {
			sb.WriteString(" UNION ")
		}
		if err := union.base.buildSelectBase(&sb); err != nil {
			return "", err
		}
	}

	if len(s.orderBys) > 0 {
		orderBySql, err := commaOrderBys(s.base.scope, s.orderBys)
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

	if s.offset != 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(s.offset))
	}

	sb.WriteString(s.lock)

	return sb.String(), nil
}

func (s selectStatus) WithContext(ctx context.Context) toSelectFinal {
	s.ctx = ctx
	return s
}

func (s selectStatus) FetchCursor() (Cursor, error) {
	sqlString, err := s.GetSQL()
	if err != nil {
		return nil, err
	}

	cursor, err := s.base.scope.Database.QueryContext(s.ctx, sqlString)
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

func (s selectStatus) FetchExactlyOne(dest ...interface{}) (err error) {
	cursor, err := s.FetchCursor()
	if err != nil {
		return
	}
	defer cursor.Close()

	hasResult := false
	for cursor.Next() {
		if hasResult {
			return errors.New("more than one rows")
		}
		err = cursor.Scan(dest...)
		if err != nil {
			return
		}
		hasResult = true
	}
	if !hasResult {
		err = errors.New("no rows")
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
