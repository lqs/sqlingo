package sqlingo

type Alias interface {
	GetSQL() string
}

type alias struct {
	expression Expression
	name       string
}

func (a *alias) GetSQL() string {
	return a.expression.GetSQL() + " AS " + getSQLForName(a.name)
}
