package generator

import (
	"database/sql"
	"errors"
	"fmt"
	"go/format"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
)

const (
	sqlingoGeneratorVersion = 2
)

type schemaFetcher interface {
	GetDatabaseName() (dbName string, err error)
	GetTableNames() (tableNames []string, err error)
	GetFieldDescriptors(tableName string) ([]fieldDescriptor, error)
	QuoteIdentifier(identifier string) string
}

type fieldDescriptor struct {
	Name      string
	Type      string
	Size      int
	Unsigned  bool
	AllowNull bool
	Comment   string
}

func convertToExportedIdentifier(s string, forceCases []string) string {
	var words []string
	nextCharShouldBeUpperCase := true
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if nextCharShouldBeUpperCase {
				words = append(words, "")
				words[len(words)-1] += string(unicode.ToUpper(r))
				nextCharShouldBeUpperCase = false
			} else {
				words[len(words)-1] += string(r)
			}
		} else {
			nextCharShouldBeUpperCase = true
		}
	}
	result := ""
	for _, word := range words {
		for _, caseWord := range forceCases {
			if strings.EqualFold(word, caseWord) {
				word = caseWord
				break
			}
		}
		result += word
	}
	var firstRune rune
	for _, r := range result {
		firstRune = r
		break
	}
	if result == "" || !unicode.IsUpper(firstRune) {
		result = "E" + result
	}
	return result
}

func getType(fieldDescriptor fieldDescriptor) (goType string, fieldClass string, fieldComment string, err error) {
	switch strings.ToLower(fieldDescriptor.Type) {
	case "tinyint":
		goType = "int8"
		fieldClass = "NumberField"
	case "smallint":
		goType = "int16"
		fieldClass = "NumberField"
	case "int", "mediumint":
		goType = "int32"
		fieldClass = "NumberField"
	case "bigint", "integer":
		goType = "int64"
		fieldClass = "NumberField"
	case "float", "double", "decimal", "real":
		goType = "float64"
		fieldClass = "NumberField"
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "date", "time", "json", "numeric", "character varying", "timestamp without time zone", "timestamp with time zone", "jsonb", "uuid":
		goType = "string"
		fieldClass = "StringField"
	case "year":
		goType = "int16"
		fieldClass = "NumberField"
		fieldDescriptor.Unsigned = true
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		// TODO: use []byte ?
		goType = "string"
		fieldClass = "StringField"
	case "array":
		// TODO: Switch to specific type instead of interface.
		goType = "string"
		fieldClass = "StringField"
	case "timestamp":
		if !timeAsString {
			goType = "time.Time"
			fieldClass = "DateField"
			fieldComment = "NOTICE: the range of timestamp is [1970-01-01 08:00:01, 2038-01-19 11:14:07]"
		} else {
			goType = "string"
			fieldClass = "StringField"
		}
	case "datetime":
		if !timeAsString {
			goType = "time.Time"
			fieldClass = "DateField"
			fieldComment = "NOTICE: the range of datetime is [0000-01-01 00:00:00, 2038-01-19 11:14:07]"
		} else {
			goType = "string"
			fieldClass = "StringField"
		}
	case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection":
		goType = "sqlingo.WellKnownBinary"
		fieldClass = "WellKnownBinaryField"
	case "bit", "bool", "boolean":
		if fieldDescriptor.Size == 1 {
			goType = "bool"
			fieldClass = "BooleanField"
		} else {
			goType = "string"
			fieldClass = "StringField"
		}
	case "user-defined":
		goType = "string"
		fieldClass = "StringField"
	default:
		err = fmt.Errorf("unknown field type %s", fieldDescriptor.Type)
		return
	}
	if fieldDescriptor.Unsigned && strings.HasPrefix(goType, "int") {
		goType = "u" + goType
	}
	if fieldDescriptor.AllowNull {
		goType = "*" + goType
	}
	return
}

func getSchemaFetcherFactory(driverName string) func(db *sql.DB) schemaFetcher {
	switch driverName {
	case "mysql":
		return newMySQLSchemaFetcher
	case "sqlite3":
		return newSQLite3SchemaFetcher
	case "postgres":
		return newPostgresSchemaFetcher
	default:
		_, _ = fmt.Fprintln(os.Stderr, "unsupported driver "+driverName)
		os.Exit(2)
		return nil
	}
}

var nonIdentifierRegexp = regexp.MustCompile(`\W`)

func ensureIdentifier(name string) string {
	result := nonIdentifierRegexp.ReplaceAllString(name, "_")
	if result == "" || (result[0] >= '0' && result[0] <= '9') {
		result = "_" + result
	}
	return result
}

// Generate generates code for the given driverName.
func Generate(driverName string, exampleDataSourceName string) (string, error) {
	options := parseArgs(exampleDataSourceName)

	db, err := sql.Open(driverName, options.dataSourceName)
	if err != nil {
		return "", err
	}
	db.SetMaxOpenConns(10)

	schemaFetcherFactory := getSchemaFetcherFactory(driverName)
	schemaFetcher := schemaFetcherFactory(db)

	dbName, err := schemaFetcher.GetDatabaseName()
	if err != nil {
		return "", err
	}

	if dbName == "" {
		return "", errors.New("no database selected")
	}

	if len(options.tableNames) == 0 {
		options.tableNames, err = schemaFetcher.GetTableNames()
		if err != nil {
			return "", err
		}
	}

	needImportTime := false
	for _, tableName := range options.tableNames {
		fieldDescriptors, err := schemaFetcher.GetFieldDescriptors(tableName)
		if err != nil {
			return "", err
		}
		for _, fieldDescriptor := range fieldDescriptors {
			if !timeAsString && fieldDescriptor.Type == "datetime" || fieldDescriptor.Type == "timestamp" {
				needImportTime = true
				break
			}
		}
	}

	code := "// This file is generated by sqlingo (https://github.com/lqs/sqlingo)\n"
	code += "// DO NOT EDIT.\n\n"
	code += "package " + ensureIdentifier(dbName) + "_dsl\n"
	if needImportTime {
		code += "import (\n"
		code += "\t\"time\"\n"
		code += "\t\"github.com/lqs/sqlingo\"\n"
		code += ")\n\n"
	} else {
		code += "import \"github.com/lqs/sqlingo\"\n\n"
	}

	code += "type sqlingoRuntimeAndGeneratorVersionsShouldBeTheSame uint32\n\n"

	sqlingoGeneratorVersionString := strconv.Itoa(sqlingoGeneratorVersion)
	code += "const _ = sqlingoRuntimeAndGeneratorVersionsShouldBeTheSame(sqlingo.SqlingoRuntimeVersion - " + sqlingoGeneratorVersionString + ")\n"
	code += "const _ = sqlingoRuntimeAndGeneratorVersionsShouldBeTheSame(" + sqlingoGeneratorVersionString + " - sqlingo.SqlingoRuntimeVersion)\n\n"

	code += "type table interface {\n"
	code += "\tsqlingo.Table\n"
	code += "}\n\n"

	code += "type numberField interface {\n"
	code += "\tsqlingo.NumberField\n"
	code += "}\n\n"

	code += "type stringField interface {\n"
	code += "\tsqlingo.StringField\n"
	code += "}\n\n"

	code += "type booleanField interface {\n"
	code += "\tsqlingo.BooleanField\n"
	code += "}\n\n"

	code += "type dateField interface {\n"
	code += "\tsqlingo.DateField\n"
	code += "}\n\n"

	var wg sync.WaitGroup

	type tableCodeItem struct {
		code string
		err  error
	}
	tableCodeMap := make(map[string]*tableCodeItem)
	fmt.Fprintln(os.Stderr, "Generating code for tables...")
	var counter int32
	for _, tableName := range options.tableNames {
		wg.Add(1)
		item := &tableCodeItem{}
		tableCodeMap[tableName] = item
		go func(tableName string) {
			defer wg.Done()
			tableCode, err := generateTable(schemaFetcher, tableName, options.forceCases)
			if err != nil {
				item.err = err
				return
			}
			_, _ = fmt.Fprintf(os.Stderr, "Generated (%d/%d) %s\n", atomic.AddInt32(&counter, 1), len(options.tableNames), tableName)
			item.code = tableCode
		}(tableName)
	}
	wg.Wait()
	for _, tableName := range options.tableNames {
		item := tableCodeMap[tableName]
		if item.err != nil {
			return "", item.err
		}
		code += item.code
	}
	code += generateGetTable(options)
	codeOut, err := format.Source([]byte(code))
	if err != nil {
		return "", err
	}
	return string(codeOut), nil
}

func generateGetTable(options options) string {
	code := "func GetTable(name string) sqlingo.Table {\n"
	code += "\tswitch name {\n"
	for _, tableName := range options.tableNames {
		code += "\tcase " + strconv.Quote(tableName) + ": return " + convertToExportedIdentifier(tableName, options.forceCases) + "\n"
	}
	code += "\tdefault: return nil\n"
	code += "\t}\n"
	code += "}\n\n"

	code += "func GetTables() []sqlingo.Table {\n"
	code += "\treturn []sqlingo.Table{\n"
	for _, tableName := range options.tableNames {
		code += "\t" + convertToExportedIdentifier(tableName, options.forceCases) + ",\n"
	}
	code += "\t}"
	code += "}\n\n"

	return code
}

func generateTable(schemaFetcher schemaFetcher, tableName string, forceCases []string) (string, error) {
	fieldDescriptors, err := schemaFetcher.GetFieldDescriptors(tableName)
	if err != nil {
		return "", err
	}

	className := convertToExportedIdentifier(tableName, forceCases)
	tableStructName := "t" + className
	tableObjectName := "o" + className

	modelClassName := className + "Model"

	tableLines := ""
	modelLines := ""
	objectLines := "\ttable: " + tableObjectName + ",\n\n"
	fieldCaseLines := ""
	classLines := ""

	fields := ""
	fieldsSQL := ""
	fullFieldsSQL := ""
	values := ""

	for _, fieldDescriptor := range fieldDescriptors {

		goName := convertToExportedIdentifier(fieldDescriptor.Name, forceCases)
		goType, fieldClass, typeComment, err := getType(fieldDescriptor)
		if err != nil {
			return "", err
		}

		privateFieldClass := string(fieldClass[0]+'a'-'A') + fieldClass[1:]

		commentLine := ""
		if fieldDescriptor.Comment != "" {
			commentLine = "\t// " + strings.ReplaceAll(fieldDescriptor.Comment, "\n", " ") + "\n"
		}
		if typeComment != "" {
			commentLine = "\t// " + typeComment + "\n"
		}

		fieldStructName := strings.ToLower(replaceTypeSpace(fieldDescriptor.Type)) + "_" + className + "_" + goName

		tableLines += commentLine
		tableLines += "\t" + goName + " " + fieldStructName + "\n"

		modelLines += commentLine
		modelLines += "\t" + goName + " " + goType + "\n"

		objectLines += commentLine
		objectLines += "\t" + goName + ": " + fieldStructName + "{"
		objectLines += "sqlingo.New" + fieldClass + "(" + tableObjectName + ", " + strconv.Quote(fieldDescriptor.Name) + ")},\n"

		fieldCaseLines += "\tcase " + strconv.Quote(fieldDescriptor.Name) + ": return t." + goName + "\n"

		classLines += "type " + fieldStructName + " struct{ " + privateFieldClass + " }\n"

		fields += "t." + goName + ", "

		if fieldsSQL != "" {
			fieldsSQL += ", "
		}
		fieldsSQL += schemaFetcher.QuoteIdentifier(fieldDescriptor.Name)

		if fullFieldsSQL != "" {
			fullFieldsSQL += ", "
		}
		fullFieldsSQL += schemaFetcher.QuoteIdentifier(tableName) + "." + schemaFetcher.QuoteIdentifier(fieldDescriptor.Name)

		values += "m." + goName + ", "
	}
	code := ""
	code += "type " + tableStructName + " struct {\n\ttable\n\n"
	code += tableLines
	code += "}\n\n"

	code += classLines

	code += "var " + tableObjectName + " = sqlingo.NewTable(" + strconv.Quote(tableName) + ")\n"
	code += "var " + className + " = " + tableStructName + "{\n"
	code += objectLines
	code += "}\n\n"

	code += "func (t t" + className + ") GetFields() []sqlingo.Field {\n"
	code += "\treturn []sqlingo.Field{" + fields + "}\n"
	code += "}\n\n"

	code += "func (t t" + className + ") GetFieldByName(name string) sqlingo.Field {\n"
	code += "\tswitch name {\n"
	code += fieldCaseLines
	code += "\tdefault: return nil\n"
	code += "\t}\n"
	code += "}\n\n"

	code += "func (t t" + className + ") GetFieldsSQL() string {\n"
	code += "\treturn " + strconv.Quote(fieldsSQL) + "\n"
	code += "}\n\n"

	code += "func (t t" + className + ") GetFullFieldsSQL() string {\n"
	code += "\treturn " + strconv.Quote(fullFieldsSQL) + "\n"
	code += "}\n\n"

	code += "type " + modelClassName + " struct {\n"
	code += modelLines
	code += "}\n\n"

	code += "func (m " + modelClassName + ") GetTable() sqlingo.Table {\n"
	code += "\treturn " + className + "\n"
	code += "}\n\n"

	code += "func (m " + modelClassName + ") GetValues() []interface{} {\n"
	code += "\treturn []interface{}{" + values + "}\n"
	code += "}\n\n"
	return code, nil
}

// replaceTypeSpace : To compatible some types contains spaces in postgresql
// like [character varying, timestamp without time zone, timestamp with time zone]
func replaceTypeSpace(typename string) string {
	typename = strings.ReplaceAll(typename, " ", "_")
	return strings.ReplaceAll(typename, "-", "_")
}
