package sqlingo

import (
	"database/sql"
	"database/sql/driver"
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
	return db
}

func init() {
	sql.Register("sqlingo-mock", &mockDriver{})
}
