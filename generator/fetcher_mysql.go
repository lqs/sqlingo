package generator

import (
	"database/sql"
	"regexp"
	"strconv"
)

type mysqlSchemaFetcher struct {
	db *sql.DB
}

func (m mysqlSchemaFetcher) GetDatabaseName() (dbName string, err error) {
	row := m.db.QueryRow("SELECT DATABASE()")
	err = row.Scan(&dbName)
	return
}

func (m mysqlSchemaFetcher) GetTableNames() (tableNames []string, err error) {
	rows, err := m.db.Query("SHOW TABLES")
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return
		}
		tableNames = append(tableNames, name)
	}
	return
}

func (m mysqlSchemaFetcher) GetFieldDescriptors(tableName string) ([]fieldDescriptor, error) {
	rows, err := m.db.Query("SHOW FULL COLUMNS FROM `" + tableName + "`")
	if err != nil {
		return nil, err
	}

	var result []fieldDescriptor
	for rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		var pointers []interface{}
		for i := 0; i < len(columns); i++ {
			var value *string
			pointers = append(pointers, &value)
		}
		err = rows.Scan(pointers...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]string)
		for i, column := range columns {
			pointer := *pointers[i].(**string)
			if pointer != nil {
				row[column] = *pointer
			}
		}

		r, _ := regexp.Compile("([a-z]+)(\\(([0-9]+)\\))?( ([a-z]+))?")
		submatches := r.FindStringSubmatch(row["Type"])

		fieldType := submatches[1]
		fieldSize := 0
		if submatches[3] != "" {
			fieldSize, err = strconv.Atoi(submatches[3])
			if err != nil {
				return nil, err
			}
		}
		unsigned := submatches[5] == "unsigned"

		result = append(result, fieldDescriptor{
			Name:      row["Field"],
			Type:      fieldType,
			Size:      fieldSize,
			Unsigned:  unsigned,
			AllowNull: row["Null"] == "YES",
			Comment:   row["Comment"],
		})
	}
	return result, nil
}

func (m mysqlSchemaFetcher) QuoteIdentifier(identifier string) string {
	return "`" + identifier + "`"
}

func newMySQLSchemaFetcher(db *sql.DB) schemaFetcher {
	return mysqlSchemaFetcher{db: db}
}
