package sqlingo

type Table interface {
	GetName() string
	GetSQL() string
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

func (t *table) GetSQL() string {
	return t.sql
}

func (t *table) getOperatorPriority() int {
	return 0
}

func NewTable(name string) Table {
	return &table{name: name, sql: getSQLForName(name)}
}
