package sqlingo

import (
	"context"
	"errors"
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

func TestWithTransaction(t *testing.T) {
	ctx := context.Background()

	// ctx without transaction
	if _, ok := ctx.Value(txContextKey{}).(Transaction); ok {
		t.Error("should not have transaction")
	}

	db := newMockDatabase()
	err := db.BeginTx(ctx, nil, func(tx Transaction) error {
		txCtx := WithTransaction(ctx, tx)
		got, ok := txCtx.Value(txContextKey{}).(Transaction)
		if !ok {
			t.Error("should have transaction")
		}
		if got.GetTx() != tx.GetTx() {
			t.Error("should be the same tx")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestWithoutTransaction(t *testing.T) {
	db := newMockDatabase()
	err := db.BeginTx(context.Background(), nil, func(tx Transaction) error {
		txCtx := WithTransaction(context.Background(), tx)
		cleanCtx := WithoutTransaction(txCtx)
		if _, ok := cleanCtx.Value(txContextKey{}).(Transaction); ok {
			t.Error("should not have transaction after WithoutTransaction")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestEnsureTx(t *testing.T) {
	db := newMockDatabase()
	sharedMockConn.mockTx = nil

	// EnsureTx should create a new transaction
	err := db.EnsureTx(context.Background(), nil, func(ctx context.Context) error {
		tx, ok := ctx.Value(txContextKey{}).(Transaction)
		if !ok || tx.GetTx() == nil {
			t.Error("should have transaction in ctx")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	if !sharedMockConn.mockTx.isCommitted {
		t.Error("should be committed")
	}

	// EnsureTx with nil ctx
	err = db.EnsureTx(nil, nil, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	// EnsureTx should rollback on error
	err = db.EnsureTx(context.Background(), nil, func(ctx context.Context) error {
		return errors.New("error")
	})
	if err == nil {
		t.Error("should get error here")
	}
	if !sharedMockConn.mockTx.isRolledBack {
		t.Error("should be rolled back")
	}

	// EnsureTx should reuse transaction from ctx
	sharedMockConn.mockTx = nil
	err = db.EnsureTx(context.Background(), nil, func(ctx context.Context) error {
		outerTx := ctx.Value(txContextKey{}).(Transaction)

		// nested EnsureTx should reuse the same tx
		return db.EnsureTx(ctx, nil, func(innerCtx context.Context) error {
			innerTx := innerCtx.Value(txContextKey{}).(Transaction)
			if innerTx.GetTx() != outerTx.GetTx() {
				t.Error("nested EnsureTx should reuse the same transaction")
			}
			return nil
		})
	})
	if err != nil {
		t.Error(err)
	}

	// EnsureTx should reuse transaction from BeginTx
	err = db.BeginTx(context.Background(), nil, func(tx Transaction) error {
		txDb := tx.(*database)
		return txDb.EnsureTx(context.Background(), nil, func(ctx context.Context) error {
			ctxTx, ok := ctx.Value(txContextKey{}).(Transaction)
			if !ok {
				t.Error("should have transaction in ctx")
			}
			if ctxTx.GetTx() != tx.GetTx() {
				t.Error("EnsureTx inside BeginTx should reuse the same transaction")
			}
			return nil
		})
	})
	if err != nil {
		t.Error(err)
	}

	// EnsureTx should fail if BeginTx fails
	sharedMockConn.beginTxError = errors.New("error")
	err = db.EnsureTx(context.Background(), nil, func(ctx context.Context) error {
		return nil
	})
	if err == nil {
		t.Error("should get error here")
	}
	sharedMockConn.beginTxError = nil
}

func TestGetTxOrDBWithContext(t *testing.T) {
	db := newMockDatabase()

	// without tx in ctx, should return db
	d := db.(*database)
	result := d.getTxOrDB(context.Background())
	if result != d.db {
		t.Error("should return db when no tx in context")
	}

	// with tx in ctx, should return tx
	err := db.BeginTx(context.Background(), nil, func(tx Transaction) error {
		txCtx := WithTransaction(context.Background(), tx)
		result := d.getTxOrDB(txCtx)
		if result != tx.GetTx() {
			t.Error("should return tx from context")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}
