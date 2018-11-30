package sqlingo

import (
	"context"
	"database/sql"
)

type Transaction interface {
	GetDB() *sql.DB
	GetTx() *sql.Tx
	Query(sql string) (Cursor, error)
	Execute(sql string) (sql.Result, error)

	Select(fields ...interface{}) SelectWithFields
	SelectDistinct(fields ...interface{}) SelectWithFields
	SelectFrom(tables ...Table) SelectWithTables
	InsertInto(table Table) InsertWithTable
	Update(table Table) UpdateWithSet
	DeleteFrom(table Table) DeleteWithTable
}

func (d *database) GetTx() *sql.Tx {
	return d.tx
}

func (d *database) BeginTx(ctx context.Context, opts *sql.TxOptions, f func(tx Transaction) error) error {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	if f != nil {
		db := *d
		db.tx = tx
		err = f(&db)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
