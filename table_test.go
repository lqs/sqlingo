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
		name: "t",
		selectStatus: selectStatus{
			base: selectBase{
				fields: dummyFields,
			},
		},
	}
	if dt.GetName() != "t" {
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
