package sqlingo

// Table is the interface of a generated table.
type Table interface {
	GetName() string
	GetSQL(scope scope) string
	GetFields() []Field
}

type actualTable interface {
	Table
	GetFieldsSQL() string
	GetFullFieldsSQL() string
}

type table struct {
	Table
	name        string
	sqlDialects dialectArray
}

func (t table) GetName() string {
	return t.name
}

func (t table) GetSQL(scope scope) string {
	return t.sqlDialects[scope.Database.dialect]
}

func (t table) getOperatorPriority() int {
	return 0
}

// NewTable creates a reference to a table. It should only be called from generated code.
func NewTable(name string) Table {
	return table{name: name, sqlDialects: quoteIdentifier(name)}
}

type derivedTable struct {
	name         string
	selectStatus selectStatus
}

func (t derivedTable) GetName() string {
	return t.name
}

func (t derivedTable) GetSQL(scope scope) string {
	sql, _ := t.selectStatus.GetSQL()
	return "(" + sql + ") AS " + t.name
}

func (t derivedTable) GetFields() []Field {
	return t.selectStatus.activeSelectBase().fields
}
