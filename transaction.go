package sqlingo

import (
	"context"
	"database/sql"
)

// Transaction is the interface of a transaction with underlying sql.Tx object.
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
