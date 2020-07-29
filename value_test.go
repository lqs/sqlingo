package sqlingo

import "testing"

func newValue(s string) value {
	return value{stringValue: &s}
}

func TestValue(t *testing.T) {
	value := newValue("42")
	if value.String() != "42" {
		t.Error()
	}
	if !value.Bool() {
		t.Error()
	}
	if value.Int() != 42 {
		t.Error()
	}
	if value.Int8() != 42 {
		t.Error()
	}
	if value.Int16() != 42 {
		t.Error()
	}
	if value.Int32() != 42 {
		t.Error()
	}
	if value.Int64() != 42 {
		t.Error()
	}

	if value.Uint() != 42 {
		t.Error()
	}
	if value.Uint8() != 42 {
		t.Error()
	}
	if value.Uint16() != 42 {
		t.Error()
	}
	if value.Uint32() != 42 {
		t.Error()
	}
	if value.Uint64() != 42 {
		t.Error()
	}
}

func TestValueOverflow1(t *testing.T) {
	value := newValue("3000000000")
	if value.Int() != 3000000000 {
		t.Error()
	}
	if value.Int8() != 0 {
		t.Error()
	}
	if value.Int16() != 0 {
		t.Error()
	}
	if value.Int32() != 0 {
		t.Error()
	}
	if value.Int64() != 3000000000 {
		t.Error()
	}

	if value.Uint() != 3000000000 {
		t.Error()
	}
	if value.Uint8() != 0 {
		t.Error()
	}
	if value.Uint16() != 0 {
		t.Error()
	}
	if value.Uint32() != 3000000000 {
		t.Error()
	}
	if value.Uint64() != 3000000000 {
		t.Error()
	}
}

func TestValueOverflow2(t *testing.T) {
	value := newValue("3000000000000000")
	if value.Int() != 3000000000000000 {
		t.Error()
	}
	if value.Int8() != 0 {
		t.Error()
	}
	if value.Int16() != 0 {
		t.Error()
	}
	if value.Int32() != 0 {
		t.Error()
	}
	if value.Int64() != 3000000000000000 {
		t.Error()
	}

	if value.Uint() != 3000000000000000 {
		t.Error()
	}
	if value.Uint8() != 0 {
		t.Error()
	}
	if value.Uint16() != 0 {
		t.Error()
	}
	if value.Uint32() != 0 {
		t.Error()
	}
	if value.Uint64() != 3000000000000000 {
		t.Error()
	}
}

func TestValueOverflow3(t *testing.T) {
	value := newValue("10000000000000000000")
	if value.Int() != 0 {
		t.Error(value.Int())
	}
	if value.Uint() != 10000000000000000000 {
		t.Error()
	}
}

func TestValueBool(t *testing.T) {
	if !newValue("1").Bool() {
		t.Error()
	}
	if newValue("0").Bool() {
		t.Error()
	}
	if newValue("").Bool() {
		t.Error()
	}
	if (value{}).Bool() {
		t.Error()
	}
}

func TestValueNull(t *testing.T) {
	value := value{}
	if value.String() != "" {
		t.Error()
	}
	if value.Int() != 0 {
		t.Error()
	}
	if value.Uint() != 0 {
		t.Error()
	}
}
