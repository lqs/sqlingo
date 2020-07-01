package sqlingo

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

func (m *mockConn) Prepare(query string) (driver.Stmt, error) {
	m.lastSql = query
	return &mockStmt{}, nil
}

func (m mockConn) Close() error {
	return nil
}

func (m *mockConn) Begin() (driver.Tx, error) {
	m.mockTx = &mockTx{}
	return m.mockTx, nil
}

var sharedMockConn = &mockConn{}

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
	db := newMockDatabase()
	if db.GetDB() == nil {
		t.Error()
	}

	interceptorExecuted := false
	loggerExecuted := false
	db.SetInterceptor(func(ctx context.Context, sql string, invoker InvokerFunc) error {
		if sql != "SELECT 1" {
			t.Error()
		}
		interceptorExecuted = true
		return invoker(ctx, sql)
	})
	db.SetLogger(func(sql string, durationNano int64) {
		if sql != "SELECT 1" {
			t.Error()
		}
		loggerExecuted = true
	})
	_, _ = db.Query("SELECT 1")
	if !interceptorExecuted || !loggerExecuted {
		t.Error()
	}

	db.SetInterceptor(func(ctx context.Context, sql string, invoker InvokerFunc) error {
		return errors.New("error")
	})
	if _, err := db.Query("SELECT 1"); err == nil {
		t.Error("should get error here")
	}
}
