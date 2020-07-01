package sqlingo

import "testing"

func TestDialect(t *testing.T) {
	nameToDialect := map[string]dialect{
		"mysql":           dialectMySQL,
		"sqlite3":         dialectSqlite3,
		"postgres":        dialectPostgres,
		"sqlserver":       dialectMSSQL,
		"mssql":           dialectMSSQL,
		"somedbidontknow": dialectUnknown,
	}

	for name, dialect := range nameToDialect {
		if getDialectFromDriverName(name) != dialect {
			t.Error()
		}
	}
}
