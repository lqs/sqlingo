package sqlingo

// OrderBy indicates the ORDER BY column and the status of descending order.
type OrderBy interface {
	GetSQL(scope scope) (string, error)
}

type orderBy struct {
	by   Expression
	desc bool
}

func (o orderBy) GetSQL(scope scope) (string, error) {
	sql, err := o.by.GetSQL(scope)
	if err != nil {
		return "", err
	}
	if o.desc {
		sql += " DESC"
	}
	return sql, nil
}
