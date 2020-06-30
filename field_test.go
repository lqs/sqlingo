package sqlingo

import "testing"

func TestField(t *testing.T) {
	assertValue(t, NewNumberField("t1", "f1").Equals(1), "`t1`.`f1` = 1")
	assertValue(t, NewBooleanField("t1", "f1").Equals(true), "`t1`.`f1` = 1")
	assertValue(t, NewStringField("t1", "f1").Equals("x"), "`t1`.`f1` = 'x'")
}
