package sqlingo

type Table interface {
	GetName() string
	GetSQL(scope scope) string
	GetFieldByName(name string) Field
	GetFields() []Field
	GetFieldsSQL() string
	GetFullFieldsSQL() string
}

type table struct {
	Table
	name string
	sql  string
}

func (t table) GetName() string {
	return t.name
}

func (t table) GetSQL(scope scope) string {
	return t.sql
}

func (t table) getOperatorPriority() int {
	return 0
}

func NewTable(name string) Table {
	return table{name: name, sql: getSQLForName(name)}
}

type derivedTable struct {
	name    string
	select_ selectStatus
}

func (t derivedTable) GetFieldsSQL() string {
	return ""
}

func (t derivedTable) GetFullFieldsSQL() string {
	return ""
}

func (t derivedTable) GetName() string {
	return t.name
}

func (t derivedTable) GetSQL(scope scope) string {
	sql, _ := t.select_.GetSQL()
	return "(" + sql + ") AS " + t.name
}

func (t derivedTable) GetFields() []Field {
	return t.select_.fields
}
