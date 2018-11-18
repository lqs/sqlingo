package sqlingo

type OrderBy interface {
	GetSQL() string
}

type orderBy struct {
	by   Expression
	desc bool
}

func (o *orderBy) GetSQL() string {
	sql := o.by.GetSQL()
	if o.desc {
		sql += " DESC"
	}
	return sql
}
