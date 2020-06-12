package sqlingo

import (
	"errors"
	"testing"
)

func TestDelete(t *testing.T) {
	errorExpression := expression{
		builder: func(scope scope) (string, error) {
			return "", errors.New("error")
		},
	}
	db := newMockDatabase()
	if _, err := db.DeleteFrom(Table1).Where(staticExpression("##", 1)).Execute(); err != nil {
		t.Error(err)
	}
	assertLastSql(t, "DELETE FROM `table1` WHERE ##")

	if _, err := db.DeleteFrom(Table1).Where(errorExpression).Execute(); err == nil {
		t.Error("should get error here")
	}
}
