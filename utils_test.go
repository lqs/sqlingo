package sqlingo

import "testing"

func assertValue(t *testing.T, value interface{}, expectedSql string) {
	t.Helper()
	if generatedSql, _, _ := getSQL(scope{}, value); generatedSql != expectedSql {
		t.Errorf("value [%v] generated [%s] expected [%s]", value, generatedSql, expectedSql)
	}
}

func assertLastSql(t *testing.T, expectedSql string) {
	t.Helper()
	if sharedMockConn.lastSql != expectedSql {
		t.Errorf("last sql [%s] expected [%s]", sharedMockConn.lastSql, expectedSql)
	}
}

func assertError(t *testing.T, value interface{}) {
	t.Helper()
	if generatedSql, _, err := getSQL(scope{}, value); err == nil {
		t.Errorf("value [%v] generated [%s] expected error", value, generatedSql)
	}
}
