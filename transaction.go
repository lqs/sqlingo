package sqlingo

import (
	"context"
	"database/sql"
	"time"
)

// Transaction is the interface of a transaction with underlying sql.Tx object.
// It provides methods to execute DDL and TCL operations.
type Transaction interface {
	GetTx() *sql.Tx
	Query(sql string) (Cursor, error)
	Execute(sql string) (sql.Result, error)

	Select(fields ...interface{}) selectWithFields
	SelectDistinct(fields ...interface{}) selectWithFields
	SelectFrom(tables ...Table) selectWithTables
	InsertInto(table Table) insertWithTable
	Update(table Table) updateWithSet
	DeleteFrom(table Table) deleteWithTable
	ReplaceInto(table Table) insertWithTable
	// ReplaceInto(table Table) insertWithTable
	Commit() error
	Rollback() error
	Savepoint(name string) error
	RollbackTo(name string) error
	ReleaseSavepoint(name string) error
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
		db := transaction{
			tx:               tx,
			logger:           d.logger,
			dialect:          d.dialect,
			retryPolicy:      d.retryPolicy,
			enableCallerInfo: d.enableCallerInfo,
			interceptor:      d.interceptor,
		}
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

// Begin starts a new transaction and returning a Transaction object.
// the DDL operations using the returned Transaction object will
// regard as one time transaction.
// User must manually call Commit() or Rollback() to end the transaction,
// after that, more DDL operations or TCL will return error.
func (d *database) Begin() (Transaction, error) {
	var err error
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	// copy extra to transaction
	t := &transaction{
		tx:               tx,
		logger:           d.logger,
		dialect:          d.dialect,
		retryPolicy:      d.retryPolicy,
		enableCallerInfo: d.enableCallerInfo,
		interceptor:      d.interceptor,
	}
	return t, nil
}

type transaction struct {
	tx               *sql.Tx
	logger           LoggerFunc
	dialect          dialect
	retryPolicy      func(error) bool
	enableCallerInfo bool
	interceptor      InterceptorFunc
}

func (t transaction) GetTx() *sql.Tx {
	return t.tx
}

func (t transaction) Query(sql string) (Cursor, error) {
	return t.QueryContext(context.Background(), sql)
}

func (t transaction) QueryContext(ctx context.Context, sqlString string) (Cursor, error) {
	isRetry := false
	for {
		sqlStringWithCallerInfo := getTxCallerInfo(t, isRetry) + sqlString

		rows, err := t.queryContextOnce(ctx, sqlStringWithCallerInfo)
		if err != nil {
			isRetry = t.tx == nil && t.retryPolicy != nil && t.retryPolicy(err)
			if isRetry {
				continue
			}
			return nil, err
		}
		return cursor{rows: rows}, nil
	}
}

func (t transaction) queryContextOnce(ctx context.Context, sqlStringWithCallerInfo string) (*sql.Rows, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		if t.logger != nil {
			t.logger(sqlStringWithCallerInfo, endTime.Sub(startTime), true, false)
		}
	}()

	interceptor := t.interceptor
	var rows *sql.Rows
	invoker := func(ctx context.Context, sql string) (err error) {
		rows, err = t.GetTx().QueryContext(ctx, sql)
		return
	}

	var err error
	if interceptor == nil {
		err = invoker(ctx, sqlStringWithCallerInfo)
	} else {
		err = interceptor(ctx, sqlStringWithCallerInfo, invoker)
	}
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (t transaction) Execute(sql string) (sql.Result, error) {
	return t.ExecuteContext(context.Background(), sql)
}

func (t transaction) ExecuteContext(ctx context.Context, sqlString string) (sql.Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sqlStringWithCallerInfo := getTxCallerInfo(t, false) + sqlString
	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		if t.logger != nil {
			t.logger(sqlStringWithCallerInfo, endTime.Sub(startTime), true, false)
		}
	}()

	var result sql.Result
	invoker := func(ctx context.Context, sql string) (err error) {
		result, err = t.GetTx().ExecContext(ctx, sql)
		return
	}
	var err error
	if t.interceptor == nil {
		err = invoker(ctx, sqlStringWithCallerInfo)
	} else {
		err = t.interceptor(ctx, sqlStringWithCallerInfo, invoker)
	}
	if err != nil {
		return nil, err
	}

	return result, err
}

func (t transaction) Select(fields ...interface{}) selectWithFields {
	return selectStatus{
		base: selectBase{
			scope: scope{
				Transaction: &t,
			},
			fields: getFields(fields),
		},
	}
}

func (t transaction) SelectDistinct(fields ...interface{}) selectWithFields {
	return selectStatus{
		base: selectBase{
			scope: scope{
				Transaction: &t,
			},
			fields:   getFields(fields),
			distinct: true,
		},
	}
}

func (t transaction) SelectFrom(tables ...Table) selectWithTables {
	return selectStatus{
		base: selectBase{
			scope: scope{
				Transaction: &t,
				Tables:      tables,
			},
		},
	}
}

func (t transaction) InsertInto(table Table) insertWithTable {
	return insertStatus{
		scope: scope{
			Transaction: &t,
			Tables:      []Table{table},
		},
	}
}

func (t transaction) Update(table Table) updateWithSet {
	return updateStatus{
		scope: scope{
			Transaction: &t,
			Tables:      []Table{table}},
	}
}

func (t transaction) DeleteFrom(table Table) deleteWithTable {
	return deleteStatus{
		scope: scope{
			Transaction: &t,
			Tables:      []Table{table},
		},
	}
}

func (t transaction) ReplaceInto(table Table) insertWithTable {
	return insertStatus{
		method: "REPLACE",
		scope: scope{
			Transaction: &t,
			Tables:      []Table{table},
		},
	}
}

func (t transaction) Commit() error {
	return t.GetTx().Commit()
}

func (t transaction) Rollback() error {
	return t.GetTx().Rollback()
}

// Savepoint todo defend sql injection
func (t transaction) Savepoint(name string) error {
	_, err := t.GetTx().Exec("SAVEPOINT " + name)
	return err
}

// RollbackTo todo defend sql injection
func (t transaction) RollbackTo(name string) error {
	_, err := t.GetTx().Exec("ROLLBACK TO " + name)
	return err
}

// ReleaseSavepoint todo defend sql injection
func (t transaction) ReleaseSavepoint(name string) error {
	_, err := t.GetTx().Exec("RELEASE SAVEPOINT " + name)
	return err
}
