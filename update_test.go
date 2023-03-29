package sqlingo

import (
	"errors"
	"testing"
)

func TestUpdate(t *testing.T) {
	db := newMockDatabase()

	_, _ = db.Update(Table1).Set(field1, field2).Where(True()).Execute()
	assertLastSql(t, "UPDATE `table1` SET `field1` = `field2` WHERE 1")

	_, _ = db.Update(Table1).
		Set(field1, 10).
		Where(field2.Equals(2)).
		OrderBy(field1.Desc()).
		Limit(2).
		Execute()
	assertLastSql(t, "UPDATE `table1` SET `field1` = 10 WHERE `field2` = 2 ORDER BY `field1` DESC LIMIT 2")

	_, _ = db.Update(Table1).
		SetIf(true, field1, 10).
		SetIf(false, field2, 10).
		Where(True()).
		Execute()
	assertLastSql(t, "UPDATE `table1` SET `field1` = 10 WHERE 1")

	_, _ = db.Update(Table1).
		SetIf(false, field1, 10).
		Where(True()).
		Execute()
	assertLastSql(t, "/* UPDATE without SET clause */ DO 0")

	_, _ = db.Update(Table1).Limit(3).Execute()
	assertLastSql(t, "/* UPDATE without SET clause */ DO 0")

	errExp := &expression{
		builder: func(scope scope) (string, error) {
			return "", errors.New("error")
		},
	}

	if _, err := db.Update(Table1).
		Set(field1, 10).
		OrderBy(orderBy{by: errExp}).
		Execute(); err == nil {
		t.Error("should get error here")
	}

	if _, err := db.Update(Table1).
		Set(field1, errExp).
		Where(True()).
		Execute(); err == nil {
		t.Error("should get error here")
	}

	if _, err := db.Update(Table1).
		Set(field1, 10).
		Where(errExp).
		Execute(); err == nil {
		t.Error("should get error here")
	}

}
