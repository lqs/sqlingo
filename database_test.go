package sqlingo

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

func (m *mockConn) Prepare(query string) (driver.Stmt, error) {
	if m.prepareError != nil {
		return nil, m.prepareError
	}
	m.lastSql = query
	return &mockStmt{
		columnCount: m.columnCount,
		rowCount:    m.rowCount,
	}, nil
}

func (m mockConn) Close() error {
	return nil
}

func (m *mockConn) Begin() (driver.Tx, error) {
	if m.beginTxError != nil {
		return nil, m.beginTxError
	}
	m.mockTx = &mockTx{}
	return m.mockTx, nil
}

var sharedMockConn = &mockConn{
	columnCount: 8,
	rowCount:    10,
}

func (m mockDriver) Open(name string) (driver.Conn, error) {
	return sharedMockConn, nil
}

func newMockDatabase() Database {
	db, err := Open("sqlingo-mock", "dummy")
	if err != nil {
		panic(err)
	}
	db.(*database).dialect = dialectMySQL
	return db
}

func init() {
	sql.Register("sqlingo-mock", &mockDriver{})
}

func TestDatabase(t *testing.T) {
	if _, err := Open("unknowndb", "unknown"); err == nil {
		t.Error()
	}

	db := newMockDatabase()
	if db.GetDB() == nil {
		t.Error()
	}

	interceptorExecutedCount := 0
	loggerExecutedCount := 0
	db.SetInterceptor(func(ctx context.Context, sql string, invoker InvokerFunc) error {
		if sql != "SELECT 1" {
			t.Error()
		}
		interceptorExecutedCount++
		return invoker(ctx, sql)
	})
	db.SetLogger(func(sql string, durationNano int64) {
		if sql != "SELECT 1" {
			t.Error(sql)
		}
		loggerExecutedCount++
	})
	_, _ = db.Query("SELECT 1")
	if interceptorExecutedCount != 1 || loggerExecutedCount != 1 {
		t.Error(interceptorExecutedCount, loggerExecutedCount)
	}

	_, _ = db.Execute("SELECT 1")
	if loggerExecutedCount != 2 {
		t.Error(loggerExecutedCount)
	}

	db.SetInterceptor(func(ctx context.Context, sql string, invoker InvokerFunc) error {
		return errors.New("error")
	})
	if _, err := db.Query("SELECT 1"); err == nil {
		t.Error("should get error here")
	}
}

func TestDatabaseRetry(t *testing.T) {
	db := newMockDatabase()
	retryCount := 0
	db.SetRetryPolicy(func(err error) bool {
		retryCount++
		return retryCount < 10
	})

	sharedMockConn.prepareError = errors.New("error")
	if _, err := db.Query("SELECT 1"); err == nil {
		t.Error("should get error here")
	}
	if retryCount != 10 {
		t.Error(retryCount)
	}
	sharedMockConn.prepareError = nil
}
