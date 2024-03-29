package sqlingo

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	// for colorful terminal print
	green = "\033[32m"
	red   = "\033[31m"
	blue  = "\033[34m"
	reset = "\033[0m"
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
	SetLogger(logger LoggerFunc)
	// Set the retry policy function.
	// The retry policy function returns true if needs retry.
	SetRetryPolicy(retryPolicy func(err error) bool)
	// EnableCallerInfo enable or disable the caller info in the log.
	// Deprecated: use SetLogger instead
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

	// Begin Start a new transaction and returning a Transaction object.
	// the DDL operations using the returned Transaction object will
	// regard as one time transaction.
	// User must manually call Commit() or Rollback() to end the transaction,
	// after that, more DDL operations or TCL will return error.
	Begin() (Transaction, error)
}

var (
	once      sync.Once
	srcPrefix string
)

type database struct {
	db               *sql.DB
	logger           LoggerFunc
	dialect          dialect
	retryPolicy      func(error) bool
	enableCallerInfo bool
	interceptor      InterceptorFunc
}

type LoggerFunc func(sql string, durationNano int64, isTx bool, retry bool)

func (d *database) SetLogger(loggerFunc LoggerFunc) {
	d.logger = loggerFunc
}

// defaultLogger is sqlingo default logger,
// which print log to stderr and regard executing time gt 100ms as slow sql.
func defaultLogger(sql string, durationNano int64, isTx bool, retry bool) {
	// for finding code position, try once is enough
	once.Do(func() {
		// $GOPATH/pkg/mod/github.com/lqs/sqlingo@vX.X.X/database.go
		_, file, _, _ := runtime.Caller(0)
		// $GOPATH/pkg/mod/github.com/lqs/sqlingo@vX.X.X
		srcPrefix = filepath.Dir(file)
	})

	var file string
	var line int
	var ok bool
	for i := 0; i < 16; i++ {
		_, file, line, ok = runtime.Caller(i)
		// `!strings.HasPrefix(file, srcPrefix)` jump out when using sqlingo as dependent package
		// `strings.HasSuffix(file, "_test.go")` jump out when executing unit test cases
		// `!ok` this is so terrible for something unexpected happened
		if !ok || !strings.HasPrefix(file, srcPrefix) || strings.HasSuffix(file, "_test.go") {
			break
		}
	}

	// convert durationNano (int64) to time.Duration
	du := time.Duration(durationNano)
	// todo shouldn't append ';' here
	if !strings.HasSuffix(sql, ";") {
		sql += ";"
	}

	sb := strings.Builder{}
	sb.Grow(32)
	sb.WriteString("|")
	sb.WriteString(du.String())
	if isTx {
		sb.WriteString("|transaction") // todo using something traceable
	}
	if retry {
		sb.WriteString("|retry")
	}
	sb.WriteString("|")

	line1 := strings.Join(
		[]string{
			"[sqlingo]",
			time.Now().Format("2006-01-02 15:04:05"),
			sb.String(),
			file + ":" + fmt.Sprint(line),
		},
		" ")

	// print to stderr
	fmt.Fprintln(os.Stderr, blue+line1+reset)
	if du < 100*time.Millisecond {
		fmt.Fprintf(os.Stderr, "%s%s%s\n", green, sql, reset)
	} else {
		fmt.Fprintf(os.Stderr, "%s%s%s\n", red, sql, reset)
	}
	fmt.Fprintln(os.Stderr)
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

// Open a database, similar to sql.Open.
// `db` using a default logger, which print log to stderr and regard executing time gt 100ms as slow sql.
// To disable the default logger, use `db.SetLogger(nil)`.
func Open(driverName string, dataSourceName string) (db Database, err error) {
	var sqlDB *sql.DB
	if dataSourceName != "" {
		sqlDB, err = sql.Open(driverName, dataSourceName)
		if err != nil {
			return
		}
	}
	db = Use(driverName, sqlDB)
	db.SetLogger(defaultLogger)
	return
}

// Use an existing *sql.DB handle
func Use(driverName string, sqlDB *sql.DB) Database {
	return &database{
		dialect: getDialectFromDriverName(driverName),
		db:      sqlDB,
	}
}

func (d database) GetDB() *sql.DB {
	return d.db
}

func (d database) Query(sqlString string) (Cursor, error) {
	return d.QueryContext(context.Background(), sqlString)
}

func (d database) QueryContext(ctx context.Context, sqlString string) (Cursor, error) {
	isRetry := false
	for {
		sqlStringWithCallerInfo := getCallerInfo(d, isRetry) + sqlString
		rows, err := d.queryContextOnce(ctx, sqlStringWithCallerInfo, isRetry)
		if err != nil {
			isRetry = d.retryPolicy != nil && d.retryPolicy(err)
			if isRetry {
				continue
			}
			return nil, err
		}
		return cursor{rows: rows}, nil
	}
}

func (d database) queryContextOnce(ctx context.Context, sqlString string, retry bool) (*sql.Rows, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	startTime := time.Now().UnixNano()
	defer func() {
		endTime := time.Now().UnixNano()
		if d.logger != nil {
			d.logger(sqlString, endTime-startTime, false, retry)
		}
	}()

	interceptor := d.interceptor
	var rows *sql.Rows
	invoker := func(ctx context.Context, sql string) (err error) {
		rows, err = d.GetDB().QueryContext(ctx, sql)
		return
	}

	var err error
	if interceptor == nil {
		err = invoker(ctx, sqlString)
	} else {
		err = interceptor(ctx, sqlString, invoker)
	}
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (d database) Execute(sqlString string) (sql.Result, error) {
	return d.ExecuteContext(context.Background(), sqlString)
}

// ExecuteContext todo Is there need retry?
func (d database) ExecuteContext(ctx context.Context, sqlString string) (sql.Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sqlStringWithCallerInfo := getCallerInfo(d, false) + sqlString
	startTime := time.Now().UnixNano()
	defer func() {
		endTime := time.Now().UnixNano()
		if d.logger != nil {
			d.logger(sqlStringWithCallerInfo, endTime-startTime, false, false)
		}
	}()

	var result sql.Result
	invoker := func(ctx context.Context, sql string) (err error) {
		result, err = d.GetDB().ExecContext(ctx, sql)
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
