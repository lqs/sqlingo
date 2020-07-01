package sqlingo

import (
	"context"
	"errors"
	"testing"
)

type mockTx struct {
	isCommitted  bool
	isRolledBack bool
}

func (m *mockTx) Commit() error {
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
		if tx.GetDB() != db.GetDB() {
			t.Error()
		}
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

}
