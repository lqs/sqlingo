package sqlingo

import (
	"database/sql"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"time"
)

type Database struct {
	db        *sql.DB
	debugMode bool
	dialect   string
}

func (d *Database) SetDebugMode(debugMode bool) {
	d.debugMode = debugMode
}

func Open(driverName string, dataSourceName string) (db *Database, err error) {
	var sqlDB *sql.DB
	if dataSourceName != "" {
		sqlDB, err = sql.Open(driverName, dataSourceName)
		if err != nil {
			return
		}
	}
	db = &Database{
		dialect: driverName,
		db:      sqlDB,
	}
	return
}

func (d *Database) GetDB() *sql.DB {
	return d.db
}

func (d *Database) Query(sql string) (Cursor, error) {
	startTime := time.Now().UnixNano()
	rows, err := d.db.Query(sql)
	endTime := time.Now().UnixNano()
	printLog(endTime-startTime, sql)
	if err != nil {
		return nil, err
	}
	return &cursor{rows: rows}, nil
}

func (d *Database) Execute(sql string) (sql.Result, error) {
	startTime := time.Now().UnixNano()
	result, err := d.db.Exec(sql)
	endTime := time.Now().UnixNano()
	printLog(endTime-startTime, sql)
	return result, err
}

var printer = message.NewPrinter(language.English)

func printLog(duration int64, sql string) {
	printer.Printf("[%9d µs] %s\n", duration/1000, sql)
}
