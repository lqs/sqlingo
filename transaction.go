package sqlingo

import (
	"context"
	"database/sql"
)

// Transaction is the interface of a transaction with underlying sql.Tx object.
// It provides methods to execute DDL and TCL operations.
type Transaction interface {
	GetDB() *sql.DB
	GetTx() *sql.Tx
	Query(sql string) (Cursor, error)
	Execute(sql string) (sql.Result, error)

	Select(fields ...interface{}) selectWithFields
	SelectDistinct(fields ...interface{}) selectWithFields
	SelectFrom(tables ...Table) selectWithTables
	InsertInto(table Table) insertWithTable
	Update(table Table) updateWithSet
	DeleteFrom(table Table) deleteWithTable
}

type txContextKey struct{}

// WithTransaction stores the transaction in the context.
func WithTransaction(ctx context.Context, tx Transaction) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx)
}

// WithoutTransaction returns a context without the transaction.
func WithoutTransaction(ctx context.Context) context.Context {
	return context.WithValue(ctx, txContextKey{}, nil)
}

func (d *database) GetTx() *sql.Tx {
	return d.tx
}

func (d *database) BeginTx(ctx context.Context, opts *sql.TxOptions, f func(tx Transaction) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	isCommitted := false
	defer func() {
		if !isCommitted {
			_ = tx.Rollback()
		}
	}()

	if f != nil {
		db := *d
		db.tx = tx
		err = f(&db)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	isCommitted = true
	return nil
}

// EnsureTx ensures the function f runs within a transaction.
// If ctx already contains a transaction started by a previous EnsureTx call, it reuses that transaction.
// Otherwise, it begins a new transaction and stores it in the context.
func (d *database) EnsureTx(ctx context.Context, opts *sql.TxOptions, f func(ctx context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(txContextKey{}).(Transaction); ok {
		return f(ctx)
	}
	if d.tx != nil {
		return f(WithTransaction(ctx, d))
	}
	return d.BeginTx(ctx, opts, func(tx Transaction) error {
		return f(WithTransaction(ctx, tx))
	})
}
