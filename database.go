package sqlingo

import (
	"context"
	"database/sql"
	"time"
)

type Database interface {
	GetDB() *sql.DB
	BeginTx(ctx context.Context, opts *sql.TxOptions, f func(tx Transaction) error) error
	Query(sql string) (Cursor, error)
	Execute(sql string) (sql.Result, error)
	SetLogger(logger func(sql string, durationNano int64))

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
	db      *sql.DB
	tx      *sql.Tx
	logger  func(sql string, durationNano int64)
	dialect string
}

func (d *database) SetLogger(logger func(sql string, durationNano int64)) {
	d.logger = logger
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
	if d.logger != nil {
		d.logger(sql, endTime-startTime)
	}
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
	if d.logger != nil {
		d.logger(sql, endTime-startTime)
	}
	return result, err
}
