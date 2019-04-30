package sqlingo

import "testing"

func assertValue(t *testing.T, value interface{}, expectedSql string) {
	if generatedSql, _, _ := getSQLFromWhatever(scope{}, value); generatedSql != expectedSql {
		t.Errorf("value [%v] generated [%s] expected [%s]", value, generatedSql, expectedSql)
	}
}

func assertError(t *testing.T, value interface{}) {
	if generatedSql, _, err := getSQLFromWhatever(scope{}, value); err == nil {
		t.Errorf("value [%v] generated [%s] expected error", value, generatedSql)
	}
}
