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
	QueryContext(ctx context.Context, sqlString string) (Cursor, error)
	Execute(sql string) (sql.Result, error)
	ExecuteContext(ctx context.Context, sql string) (sql.Result, error)
	SetLogger(logger func(sql string, durationNano int64))
	SetRetryPolicy(retryPolicy func(err error) bool)
	EnableCallerInfo(enableCallerInfo bool)
	SetInterceptor(interceptor InterceptorFunc)

	Select(fields ...interface{}) selectWithFields
	SelectDistinct(fields ...interface{}) selectWithFields
	SelectFrom(tables ...Table) selectWithTables
	InsertInto(table Table) insertWithTable
	ReplaceInto(table Table) insertWithTable
	Update(table Table) updateWithSet
	DeleteFrom(table Table) deleteWithTable
}

type txOrDB interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type database struct {
	db               *sql.DB
	tx               *sql.Tx
	logger           func(sql string, durationNano int64)
	dialect          dialect
	retryPolicy      func(error) bool
	enableCallerInfo bool
	interceptor      InterceptorFunc
}

func (d *database) SetLogger(logger func(sql string, durationNano int64)) {
	d.logger = logger
}

func (d *database) SetRetryPolicy(retryPolicy func(err error) bool) {
	d.retryPolicy = retryPolicy
}

func (d *database) EnableCallerInfo(enableCallerInfo bool) {
	d.enableCallerInfo = enableCallerInfo
}

func (d *database) SetInterceptor(interceptor InterceptorFunc) {
	d.interceptor = interceptor
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
		dialect: getDialectFromDriverName(driverName),
		db:      sqlDB,
	}
	return
}

func (d database) GetDB() *sql.DB {
	return d.db
}

func (d database) getTxOrDB() txOrDB {
	if d.tx != nil {
		return d.tx
	} else {
		return d.db
	}
}

func (d database) Query(sqlString string) (Cursor, error) {
	return d.QueryContext(context.Background(), sqlString)
}

func (d database) QueryContext(ctx context.Context, sqlString string) (Cursor, error) {
	isRetry := false
	for {
		sqlStringWithCallerInfo := getCallerInfo(d, isRetry) + sqlString

		rows, err := d.queryContextOnce(ctx, sqlStringWithCallerInfo)
		if err != nil {
			isRetry = d.tx == nil && d.retryPolicy != nil && d.retryPolicy(err)
			if isRetry {
				continue
			}
			return nil, err
		}
		return cursor{rows: rows}, nil
	}
}

func (d database) queryContextOnce(ctx context.Context, sqlStringWithCallerInfo string) (*sql.Rows, error) {
	startTime := time.Now().UnixNano()
	defer func() {
		endTime := time.Now().UnixNano()
		if d.logger != nil {
			d.logger(sqlStringWithCallerInfo, endTime-startTime)
		}
	}()

	interceptor := d.interceptor
	var rows *sql.Rows
	invoker := func(ctx context.Context, sql string) (err error) {
		rows, err = d.getTxOrDB().QueryContext(ctx, sql)
		return
	}

	var err error
	if interceptor == nil {
		err = invoker(ctx, sqlStringWithCallerInfo)
	} else {
		err = interceptor(ctx, sqlStringWithCallerInfo, invoker)
	}
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (d database) Execute(sql string) (sql.Result, error) {
	return d.ExecuteContext(context.Background(), sql)
}

func (d database) ExecuteContext(ctx context.Context, sql string) (sql.Result, error) {
	sql = getCallerInfo(d, false) + sql
	startTime := time.Now().UnixNano()
	result, err := d.getTxOrDB().ExecContext(ctx, sql)
	endTime := time.Now().UnixNano()
	if d.logger != nil {
		d.logger(sql, endTime-startTime)
	}
	return result, err
}
