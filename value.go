package sqlingo

import (
	"math"
	"strconv"
)

const (
	maxInt  = 1<<(strconv.IntSize-1) - 1
	minInt  = -1 << (strconv.IntSize - 1)
	maxUint = 1<<strconv.IntSize - 1
)

type value struct {
	stringValue *string
}

func (v value) Int64() int64 {
	if v.stringValue == nil {
		return 0
	}
	// TODO: check BIT(1)
	if result, err := strconv.ParseInt(*v.stringValue, 10, 64); err == nil {
		return result
	}
	return 0
}

func (v value) Uint64() uint64 {
	if v.stringValue == nil {
		return 0
	}
	// TODO: check BIT(1)
	if result, err := strconv.ParseUint(*v.stringValue, 10, 64); err == nil {
		return result
	}
	return 0
}

func (v value) Int() int {
	if r := v.Int64(); r >= minInt && r <= maxInt {
		return int(r)
	}
	return 0
}

func (v value) Int8() int8 {
	if r := v.Int64(); r >= math.MinInt8 && r <= math.MaxInt8 {
		return int8(r)
	}
	return 0
}

func (v value) Int16() int16 {
	if r := v.Int64(); r >= math.MinInt16 && r <= math.MaxInt16 {
		return int16(r)
	}
	return 0
}

func (v value) Int32() int32 {
	if r := v.Int64(); r >= math.MinInt32 && r <= math.MaxInt32 {
		return int32(r)
	}
	return 0
}

func (v value) Uint() uint {
	if r := v.Uint64(); r <= maxUint {
		return uint(r)
	}
	return 0
}

func (v value) Uint8() uint8 {
	if r := v.Uint64(); r <= math.MaxUint8 {
		return uint8(r)
	}
	return 0
}

func (v value) Uint16() uint16 {
	if r := v.Uint64(); r <= math.MaxUint16 {
		return uint16(r)
	}
	return 0
}

func (v value) Uint32() uint32 {
	if r := v.Uint64(); r <= math.MaxUint32 {
		return uint32(r)
	}
	return 0
}

func (v value) Bool() bool {
	if v.stringValue == nil {
		return false
	}
	switch *v.stringValue {
	case "", "0", "\x00":
		return false
	default:
		return true
	}
}

func (v value) String() string {
	if v.stringValue == nil {
		return ""
	}
	return *v.stringValue
}

func (v value) IsNull() bool {
	return v.stringValue == nil
}
