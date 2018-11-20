package sqlingo

import (
	"database/sql"
	"errors"
	"fmt"
	"go/format"
	"regexp"
	"strconv"
	"unicode"
)

func convertCase(s string) (result string) {
	nextCharShouldBeUpperCase := true
	for _, ch := range s {
		if ch == '_' {
			nextCharShouldBeUpperCase = true
		} else {
			if nextCharShouldBeUpperCase {
				result += string(unicode.ToUpper(ch))
				nextCharShouldBeUpperCase = false
			} else {
				result += string(ch)
			}
		}
	}
	return
}

func getType(s string, nullable bool) (goType string, fieldClass string, err error) {
	r, _ := regexp.Compile("([a-z]+)(\\(([0-9]+)\\))?")

	submatches := r.FindStringSubmatch(s)
	fieldType := submatches[1]
	fieldSize := 0
	if submatches[3] != "" {
		fieldSize, err = strconv.Atoi(submatches[3])
		if err != nil {
			return
		}
	}
	switch fieldType {
	case "tinyint":
		goType = "int8"
		fieldClass = "NumberField"
	case "smallint":
		goType = "int16"
		fieldClass = "NumberField"
	case "int", "mediumint":
		goType = "int32"
		fieldClass = "NumberField"
	case "bigint":
		goType = "int64"
		fieldClass = "NumberField"
	case "float", "double", "decimal":
		goType = "float64"
		fieldClass = "NumberField"
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "datetime", "date", "time", "timestamp":
		goType = "string"
		fieldClass = "StringField"
	case "blob", "tinyblob", "mediumblob", "longblob":
		goType = "[]byte"
		fieldClass = "StringField"
	case "bit":
		if fieldSize == 1 {
			goType = "bool"
			fieldClass = "BooleanField"
		} else {
			goType = "string"
			fieldClass = "StringField"
		}
	default:
		err = fmt.Errorf("unknown field type %s", fieldType)
		return
	}
	if nullable {
		goType = "*" + goType
	}
	return
}

func wrapQuote(s string) string {
	return "\"" + s + "\""
}

func Generate(driverName string, dataSourceName string, tableNames *[]string) (string, error) {

	mysql, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return "", err
	}

	rows, err := mysql.Query("SELECT DATABASE()")
	if err != nil {
		return "", err
	}
	var dbName *string
	if rows.Next() {
		err := rows.Scan(&dbName)
		if err != nil {
			return "", err
		}
	}

	if dbName == nil {
		return "", errors.New("no database selected")
	}

	code := "package " + *dbName + "_dsl\n"
	code += "import . \"github.com/lqs/sqlingo\"\n"

	if tableNames == nil {
		tableNames = &[]string{}
		rows, err := mysql.Query("SHOW TABLES")
		if err != nil {
			return "", err
		}
		for rows.Next() {
			var name string
			rows.Scan(&name)
			*tableNames = append(*tableNames, name)
		}
	}

	for _, tableName := range *tableNames {
		println("Generating", tableName)
		rows, err := mysql.Query("SHOW FULL COLUMNS FROM " + getSQLForName(tableName))
		if err != nil {
			return "", err
		}

		tableLines := ""
		modelLines := ""
		objectLines := "\tTable: NewTable(\"" + tableName + "\"),\n"
		classLines := ""

		className := convertCase(tableName)
		tableStructName := "t" + className

		modelClassName := className + "Model"

		fields := ""
		values := ""

		for rows.Next() {
			columns, err := rows.Columns()
			if err != nil {
				return "", err
			}
			var pointers []interface{}
			for i := 0; i < len(columns); i++ {
				var value *string
				pointers = append(pointers, &value)
			}
			err = rows.Scan(pointers...)
			if err != nil {
				return "", err
			}
			row := make(map[string]string)
			for i, column := range columns {
				pointer := *pointers[i].(**string)
				if pointer != nil {
					row[column] = *pointer
				}
			}

			goName := convertCase(row["Field"])
			goType, fieldClass, err := getType(row["Type"], row["Null"] == "YES")
			if err != nil {
				return "", err
			}

			fieldStructName := "f" + className + goName

			tableLines += "\t" + goName + " " + fieldStructName + "\n"
			modelLines += "\t" + goName + " " + goType + "\n"
			objectLines += "\t" + goName + ": " + fieldStructName + "{"
			objectLines += "New" + fieldClass + "(" + wrapQuote(tableName) + ", " + wrapQuote(row["Field"]) + ")},\n"
			classLines += "type " + fieldStructName + " struct{ " + fieldClass + " }\n"

			fields += "t." + goName + ", "
			values += "m." + goName + ", "
		}

		//println(tableLines, recordLines, objectLines, classLines)

		code += "type " + tableStructName + " struct {\n\tTable\n"
		code += tableLines
		code += "}\n\n"

		code += classLines

		code += "var " + className + " = " + tableStructName + "{\n"
		code += objectLines
		code += "}\n\n"

		code += "func (t t" + className + ") GetFields() []Field {\n"
		code += "\treturn []Field{" + fields + "}\n"
		code += "}\n\n"

		code += "type " + modelClassName + " struct {\n"
		//code += "\tModel\n"
		code += modelLines
		code += "}\n\n"

		code += "func (m " + modelClassName + ") GetTable() Table {\n"
		code += "\treturn " + className + "\n"
		code += "}\n\n"

		code += "func (m " + modelClassName + ") GetValues() []interface{} {\n"
		code += "\treturn []interface{}{" + values + "}\n"
		code += "}\n\n"
	}

	codeOut, err := format.Source([]byte(code))
	if err != nil {
		return "", err
	}

	return string(codeOut), nil
}
