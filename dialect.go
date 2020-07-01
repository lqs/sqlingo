package sqlingo

type dialect int

const (
	dialectUnknown dialect = iota
	dialectMySQL
	dialectSqlite3
	dialectPostgres
	dialectMSSQL

	dialectCount
)

type dialectArray [dialectCount]string

func getDialectFromDriverName(driverName string) dialect {
	switch driverName {
	case "mysql":
		return dialectMySQL
	case "sqlite3":
		return dialectSqlite3
	case "postgres":
		return dialectPostgres
	case "sqlserver", "mssql":
		return dialectMSSQL
	default:
		return dialectUnknown
	}
}
