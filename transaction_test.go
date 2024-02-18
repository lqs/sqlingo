package sqlingo

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-playground/assert/v2"
	"testing"
)

type mockTx struct {
	isCommitted  bool
	isRolledBack bool
	commitError  error
}

func (m *mockTx) Commit() error {
	if m.commitError != nil {
		return m.commitError
	}
	m.isCommitted = true
	return nil
}

func (m *mockTx) Rollback() error {
	m.isRolledBack = true
	return nil
}

func TestTransaction(t *testing.T) {
	db := newMockDatabase()
	err := db.BeginTx(nil, nil, func(tx Transaction) error {
		if tx.GetTx() == nil {
			t.Error()
		}

		_, err := tx.Execute("<dummy>")
		if err != nil {
			t.Error(err)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	if !sharedMockConn.mockTx.isCommitted {
		t.Error()
	}
	if sharedMockConn.mockTx.isRolledBack {
		t.Error()
	}

	err = db.BeginTx(context.Background(), nil, func(tx Transaction) error {
		return errors.New("error")
	})
	if err == nil {
		t.Error("should get error here")
	}
	if sharedMockConn.mockTx.isCommitted {
		t.Error()
	}
	if !sharedMockConn.mockTx.isRolledBack {
		t.Error()
	}

	sharedMockConn.beginTxError = errors.New("error")
	err = db.BeginTx(context.Background(), nil, func(tx Transaction) error {
		return nil
	})
	if err == nil {
		t.Error("should get error here")
	}
	sharedMockConn.beginTxError = nil

	err = db.BeginTx(context.Background(), nil, func(tx Transaction) error {
		sharedMockConn.mockTx.commitError = errors.New("error")
		return nil
	})
	if err == nil {
		t.Error("should get error here")
	}
}

func TestTransaction_Commit(t *testing.T) {
	db := newMockDatabase()
	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
	}

	if err = tx.Commit(); err != nil {
		t.Error(err)
	}

	if !sharedMockConn.mockTx.isCommitted {
		t.Error()
	}
}

func TestTransaction_Rollback(t *testing.T) {
	db := newMockDatabase()
	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
	}

	if err = tx.Rollback(); err != nil {
		t.Error(err)
	}
	if !sharedMockConn.mockTx.isRolledBack {
		t.Error()
	}
}

func TestTransaction_Done(t *testing.T) {
	db := newMockDatabase()
	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
	}

	if err = tx.Commit(); err != nil {
		t.Error(err)
	}

	if err = tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
		t.Error(err)
	}

	if err = tx.Commit(); !errors.Is(err, sql.ErrTxDone) {
		t.Error(err)
	}

	if _, err = tx.Select(1).FetchAll(); !errors.Is(err, sql.ErrTxDone) {
		t.Error(err)
	}
}

func TestTransaction_Execute(t *testing.T) {
	var sqlCount = make(map[string]int)
	db := newMockDatabase()

	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
	}
	db.SetInterceptor(func(ctx context.Context, sql string, invoker InvokerFunc) error {
		sqlCount[sql]++
		return invoker(ctx, sql)
	})

	if _, err = tx.Execute("SQL 1 NOT SET INTERCEPTOR"); err != nil {
		t.Error(err)
	}
	assert.Equal(t, sqlCount["SQL 1 NOT SET INTERCEPTOR"], 0)

	if err = tx.Rollback(); err != nil {
		t.Error(err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Error(err)
	}
	if _, err = tx.Execute("SQL 2 SET INTERCEPTOR"); err != nil {
		t.Error(err)
	}
	assert.Equal(t, sqlCount["SQL 2 SET INTERCEPTOR"], 1)

	if err = tx.Commit(); err != nil {
		t.Error(err)
	}
}

// TestTransaction_CRUD tests the CRUD operations in a transaction, cause sql build is tested on database,
// so we only insure there is no panic here.
func TestTransaction_CRUD(t *testing.T) {
	db := newMockDatabase()
	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
	}
	_, err = tx.Select().From(table1).FetchAll()
	if err != nil {
		t.Error(err)
	}

	if _, err = tx.SelectFrom(table1).FetchAll(); err != nil {
		t.Error(err)
	}

	if _, err = tx.SelectDistinct(field2).From(table1).FetchAll(); err != nil {
		t.Error(err)
	}

	if _, err = tx.InsertInto(Test).Values(1, 2).Execute(); err != nil {
		t.Error(err)
	}

	if _, err = tx.ReplaceInto(Test).Values(1, 2).Execute(); err != nil {
		t.Error(err)
	}

	if _, err = tx.DeleteFrom(table1).Where().Execute(); err != nil {
		t.Error(err)
	}

	if _, err = tx.Update(table1).Set(field1, 1).Where().Execute(); err != nil {
		t.Error(err)
	}

	if err = tx.Rollback(); err != nil {
		t.Error(err)
	}

}
