package sqlingo

import (
	"context"
	"database/sql"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"time"
)

type Database interface {
	SetDebugMode(debugMode bool)
	GetDB() *sql.DB
	BeginTx(ctx context.Context, opts *sql.TxOptions, f func(tx Transaction) error) error
	Query(sql string) (Cursor, error)
	Execute(sql string) (sql.Result, error)

	Select(fields ...interface{}) SelectWithFields
	SelectDistinct(fields ...interface{}) SelectWithFields
	SelectFrom(tables ...Table) SelectWithTables
	InsertInto(table Table) InsertWithTable
	Update(table Table) UpdateWithSet
	DeleteFrom(table Table) DeleteWithTable
}

type txOrDB interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type database struct {
	db        *sql.DB
	tx        *sql.Tx
	debugMode bool
	dialect   string
}

func (d *database) SetDebugMode(debugMode bool) {
	d.debugMode = debugMode
}

func Open(driverName string, dataSourceName string) (db Database, err error) {
	var sqlDB *sql.DB
	if dataSourceName != "" {
		sqlDB, err = sql.Open(driverName, dataSourceName)
		if err != nil {
			return
		}
	}
	db = &database{
		dialect: driverName,
		db:      sqlDB,
	}
	return
}

func (d *database) GetDB() *sql.DB {
	return d.db
}

func (d *database) getTxOrDB() txOrDB {
	if d.tx != nil {
		return d.tx
	} else {
		return d.db
	}
}

func (d *database) Query(sql string) (Cursor, error) {
	sql = getCallerInfo(d) + sql
	startTime := time.Now().UnixNano()
	rows, err := d.getTxOrDB().Query(sql)
	endTime := time.Now().UnixNano()
	printLog(endTime-startTime, sql)
	if err != nil {
		return nil, err
	}
	return cursor{rows: rows}, nil
}

func (d *database) Execute(sql string) (sql.Result, error) {
	sql = getCallerInfo(d) + sql
	startTime := time.Now().UnixNano()
	result, err := d.getTxOrDB().Exec(sql)
	endTime := time.Now().UnixNano()
	printLog(endTime-startTime, sql)
	return result, err
}

var printer = message.NewPrinter(language.English)

func printLog(duration int64, sql string) {
	printer.Printf("[%9d µs] %s\n", duration/1000, sql)
}
