package sqlingo

import "testing"

func TestGeometry(t *testing.T) {
	assertValue(t, STGeomFromText("sample wkt"), "ST_GeomFromText('sample wkt')")
	assertValue(t, STGeomFromTextf("sample wkt %d", 1), "ST_GeomFromText('sample wkt 1')")

	e := expression{
		builder: func(scope scope) (string, error) {
			return "<>", nil
		},
	}
	assertValue(t, e.STAsText(), "ST_AsText(<>)")

	t1 := NewTable("t1")
	field := NewWellKnownBinaryField(t1, "f1")
	assertValue(t, field, "`t1`.`f1`")
}
