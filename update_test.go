package sqlingo

import "testing"

func TestUpdate(t *testing.T) {
	db := database{}

	assertValue(t, db.Update(Table1).Set(field1, field2),
		"UPDATE `table1` SET `field1` = `field2`")

	assertValue(t, db.Update(Table1).Set(field1, 10).Where(field2.Equals(2)).OrderBy(field1.Desc()).Limit(2),
		"UPDATE `table1` SET `field1` = 10 WHERE `field2` = 2 ORDER BY `field1` DESC LIMIT 2")
}
