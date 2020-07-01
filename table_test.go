package sqlingo

import "testing"

func TestTable(t *testing.T) {
	table := table{}
	if table.getOperatorPriority() != 0 {
		t.Error()
	}
}

func TestDerivedTable(t *testing.T) {
	dummyFields := []Field{NewNumberField("table", "field")}
	dt := derivedTable{
		select_: selectStatus{
			fields: dummyFields,
		},
	}
	if dt.GetFieldByName("dummy") != nil {
		t.Error()
	}
	if dt.GetFieldsSQL() != "" {
		t.Error()
	}
	if dt.GetFullFieldsSQL() != "" {
		t.Error()
	}
	if dt.GetName() != "" {
		t.Error()
	}

	sql, err := dt.GetFields()[0].GetSQL(dummyMySQLScope)
	if err != nil {
		t.Error(err)
	}
	if sql != "`table`.`field`" {
		t.Error(sql)
	}
}
