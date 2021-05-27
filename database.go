package sqlingo

import (
	"context"
	"database/sql"
	"time"
)

// Database is the interface of a database with underlying sql.DB object.
type Database interface {
	// Get the underlying sql.DB object of the database
	GetDB() *sql.DB
	BeginTx(ctx context.Context, opts *sql.TxOptions, f func(tx Transaction) error) error
	// Executes a query and return the cursor
	Query(sql string) (Cursor, error)
	// Executes a query with context and return the cursor
	QueryContext(ctx context.Context, sqlString string) (Cursor, error)
	// Executes a statement
	Execute(sql string) (sql.Result, error)
	// Executes a statement with context
	ExecuteContext(ctx context.Context, sql string) (sql.Result, error)
	// Set the logger function
	SetLogger(logger func(sql string, durationNano int64))
	// Set the retry policy function.
	// The retry policy function returns true if needs retry.
	SetRetryPolicy(retryPolicy func(err error) bool)
	// enable or disable caller info
	EnableCallerInfo(enableCallerInfo bool)
	// Set a interceptor function
	SetInterceptor(interceptor InterceptorFunc)

	// Initiate a SELECT statement
	Select(fields ...interface{}) selectWithFields
	// Initiate a SELECT DISTINCT statement
	SelectDistinct(fields ...interface{}) selectWithFields
	// Initiate a SELECT * FROM statement
	SelectFrom(tables ...Table) selectWithTables
	// Initiate a INSERT INTO statement
	InsertInto(table Table) insertWithTable
	// Initiate a REPLACE INTO statement
	ReplaceInto(table Table) insertWithTable
	// Initiate a UPDATE statement
	Update(table Table) updateWithSet
	// Initiate a DELETE FROM statement
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

// Open a database, similar to sql.Open
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
	}
	return d.db
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
	if ctx == nil {
		ctx = context.Background()
	}
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

func (d database) Execute(sqlString string) (sql.Result, error) {
	return d.ExecuteContext(context.Background(), sqlString)
}

func (d database) ExecuteContext(ctx context.Context, sqlString string) (sql.Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sqlStringWithCallerInfo := getCallerInfo(d, false) + sqlString
	startTime := time.Now().UnixNano()
	defer func() {
		endTime := time.Now().UnixNano()
		if d.logger != nil {
			d.logger(sqlStringWithCallerInfo, endTime-startTime)
		}
	}()

	var result sql.Result
	invoker := func(ctx context.Context, sql string) (err error) {
		result, err = d.getTxOrDB().ExecContext(ctx, sql)
		return
	}
	var err error
	if d.interceptor == nil {
		err = invoker(ctx, sqlStringWithCallerInfo)
	} else {
		err = d.interceptor(ctx, sqlStringWithCallerInfo, invoker)
	}
	if err != nil {
		return nil, err
	}

	return result, err
}
