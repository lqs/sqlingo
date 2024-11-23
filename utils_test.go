package sqlingo

import "testing"

var dummyMySQLScope = scope{Database: &database{dialect: dialectMySQL}}

func assertEqual(t *testing.T, actualValue string, expectedValue string) {
	t.Helper()
	if actualValue != expectedValue {
		t.Errorf("actual [%s] expected [%s]", actualValue, expectedValue)
	}
}

func assertValue(t *testing.T, value interface{}, expectedSql string) {
	t.Helper()
	if generatedSql, _, _ := getSQL(dummyMySQLScope, value); generatedSql != expectedSql {
		t.Errorf("value [%v] generated [%s] expected [%s]", value, generatedSql, expectedSql)
	}
}

func assertLastSql(t *testing.T, expectedSql string) {
	t.Helper()
	if sharedMockConn.lastSql != expectedSql {
		t.Errorf("last sql [%s] expected [%s]", sharedMockConn.lastSql, expectedSql)
	}
	sharedMockConn.lastSql = ""
}

func assertError(t *testing.T, value interface{}) {
	t.Helper()
	if generatedSql, _, err := getSQL(dummyMySQLScope, value); err == nil {
		t.Errorf("value [%v] generated [%s] expected error", value, generatedSql)
	}
}
