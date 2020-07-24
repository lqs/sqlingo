package sqlingo

import "testing"

func TestValue(t *testing.T) {
	v := "42"
	value := value{stringValue: &v}
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
	v := "3000000000"
	value := value{stringValue: &v}
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
	v := "3000000000000000"
	value := value{stringValue: &v}
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
	v := "10000000000000000000"
	value := value{stringValue: &v}
	if value.Int() != 0 {
		t.Error(value.Int())
	}
	if value.Uint() != 10000000000000000000 {
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
