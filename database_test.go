package sqlingo

import (
	"database/sql"
	"database/sql/driver"
	"errors"
)

func (m mockConn) Prepare(query string) (driver.Stmt, error) {
	return &mockStmt{}, nil
}

func (m mockConn) Close() error {
	return nil
}

func (m mockConn) Begin() (driver.Tx, error) {
	return nil, errors.New("tx not implemented in mock")
}

func (m mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConn{}, nil
}

func init() {
	sql.Register("sqlingo-mock", &mockDriver{})
}
