package generator

import "database/sql"

type sqlite3SchemaFetcher struct {
	db *sql.DB
}

func (s sqlite3SchemaFetcher) GetDatabaseName() (dbName string, err error) {
	dbName = "main"
	return
}

func (s sqlite3SchemaFetcher) GetTableNames() (tableNames []string, err error) {
	rows, err := s.db.Query("SELECT `name` FROM `sqlite_master` WHERE `type` ='table' AND `name` NOT LIKE 'sqlite_%'")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return
		}
		tableNames = append(tableNames, name)
	}
	return
}

func (s sqlite3SchemaFetcher) GetFieldDescriptors(tableName string) (result []fieldDescriptor, err error) {
	rows, err := s.db.Query("SELECT `name`, `type`, `notnull` FROM pragma_table_info('" + tableName + "')")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var fieldDescriptor fieldDescriptor
		var notNull int
		if err = rows.Scan(&fieldDescriptor.Name, &fieldDescriptor.Type, &notNull); err != nil {
			return
		}
		fieldDescriptor.AllowNull = notNull == 0
		result = append(result, fieldDescriptor)
	}
	return
}

func (s sqlite3SchemaFetcher) QuoteIdentifier(identifier string) string {
	return "\"" + identifier + "\""
}

func newSQLite3SchemaFetcher(db *sql.DB) schemaFetcher {
	return sqlite3SchemaFetcher{db: db}
}
