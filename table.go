package sqlingo

type Table interface {
	GetName() string
	GetSQL(scope scope) string
	GetFields() []Field
}

type table struct {
	Table
	name string
	sql  string
}

func (t *table) GetName() string {
	return t.name
}

func (t *table) GetSQL(scope scope) string {
	return t.sql
}

func (t *table) getOperatorPriority() int {
	return 0
}

func NewTable(name string) Table {
	return &table{name: name, sql: getSQLForName(name)}
}
