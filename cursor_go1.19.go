//go:build !go1.20 && !go1.21

package sqlingo

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// Cursor is the interface of a row cursor.
type Cursor interface {
	Next() bool
	Scan(dest ...interface{}) error
	GetMap() (map[string]value, error)
	Close() error
}

type cursor struct {
	rows *sql.Rows
}

func (c cursor) Next() bool {
	return c.rows.Next()
}

var timeType = reflect.TypeOf(time.Time{})

var timeLayouts = []string{
	"2006-01-02",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.000",
	"2006-01-02 15:04:05.000000",
	"2006-01-02 15:04:05.000000000",
	time.RFC3339Nano,
}

func parseTime(s string) (time.Time, error) {
	for _, layout := range timeLayouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unknown time format %s", s)
}

func isScanner(val reflect.Value) bool {
	_, ok := val.Addr().Interface().(sql.Scanner)
	return ok
}

func preparePointers(val reflect.Value, scans *[]interface{}) error {
	kind := val.Kind()
	switch kind {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		if addr := val.Addr(); addr.CanInterface() {
			*scans = append(*scans, addr.Interface())
		}
	case reflect.Struct:
		if canScan := val.Type() == timeType || isScanner(val); canScan {
			*scans = append(*scans, val.Addr().Interface())
			return nil
		}
		for j := 0; j < val.NumField(); j++ {
			field := val.Field(j)
			if field.Kind() == reflect.Interface {
				continue
			}
			if err := preparePointers(field, scans); err != nil {
				return err
			}
		}
	case reflect.Ptr:
		toType := val.Type().Elem()
		switch toType.Kind() {
		case reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64,
			reflect.String:
			*scans = append(*scans, val.Addr().Interface())
		case reflect.Struct:
			if toType == reflect.TypeOf(time.Time{}) {
				*scans = append(*scans, val.Addr().Interface())
			} else {
				to := reflect.New(toType).Elem()
				val.Set(to.Addr())
				err := preparePointers(to, scans)
				if err != nil {
					return nil
				}
			}
		default:
			to := reflect.New(toType).Elem()
			val.Set(to.Addr())
			err := preparePointers(to, scans)
			if err != nil {
				return nil
			}
		}
	case reflect.Slice:
		if _, ok := (val.Interface()).([]byte); ok {
			*scans = append(*scans, val.Addr().Interface())
		} else {
			return fmt.Errorf("unknown type []%s", val.Type().Elem().Kind().String())
		}
	default:
		return fmt.Errorf("unknown type %s", kind.String())
	}
	return nil
}

func parseBool(s []byte) (bool, error) {
	if len(s) == 1 {
		if s[0] == 0 {
			return false, nil
		} else if s[0] == 1 {
			return true, nil
		}
	}
	return strconv.ParseBool(string(s))
}

func (c cursor) Scan(dest ...interface{}) error {
	columns, err := c.rows.Columns()
	if err != nil {
		return err
	}
	values := make([]interface{}, len(columns))
	pointers := make([]interface{}, len(columns))
	for i := range columns {
		pointers[i] = &values[i]
	}
	if err := c.rows.Scan(pointers...); err != nil {
		return err
	}

	if len(dest) == 0 {
		// dry run
		return nil
	}

	var scans []interface{}
	for i, item := range dest {
		if reflect.ValueOf(item).Kind() != reflect.Ptr {
			return fmt.Errorf("argument %d is not pointer", i)
		}

		val := reflect.Indirect(reflect.ValueOf(item))

		err := preparePointers(val, &scans)
		if err != nil {
			return err
		}
	}

	pbs := make(map[int]*bool)
	ppbs := make(map[int]**bool)
	pts := make(map[int]*time.Time)
	ppts := make(map[int]**time.Time)

	for i, scan := range scans {
		switch scan.(type) {
		case *bool:
			var s []uint8
			scans[i] = &s
			pbs[i] = scan.(*bool)
		case **bool:
			var s *[]uint8
			scans[i] = &s
			ppbs[i] = scan.(**bool)
		case *time.Time:
			var s string
			scans[i] = &s
			pts[i] = scan.(*time.Time)
		case **time.Time:
			var s sql.NullString
			scans[i] = &s
			ppts[i] = scan.(**time.Time)
		}
	}

	if err := c.rows.Scan(scans...); err != nil {
		return err
	}

	for i, pb := range pbs {
		if *(scans[i].(*[]byte)) == nil {
			return fmt.Errorf("field %d is null", i)
		}
		b, err := parseBool(*(scans[i].(*[]byte)))
		if err != nil {
			return err
		}
		*pb = b
	}
	for i, ppb := range ppbs {
		if *(scans[i].(**[]uint8)) == nil {
			*ppb = nil
		} else {
			b, err := parseBool(**(scans[i].(**[]byte)))
			if err != nil {
				return err
			}
			*ppb = &b
		}
	}
	for i := range pts {
		s := scans[i].(*string)
		if s == nil {
			return fmt.Errorf("field %d is null", i)
		}
		t, err := parseTime(*s)
		if err != nil {
			return err
		}
		*pts[i] = t

	}
	for i := range ppts {
		nullString := scans[i].(*sql.NullString)
		if nullString == nil {
			return fmt.Errorf("field %d is null", i)
		}
		if !nullString.Valid {
			*ppts[i] = nil
		} else {
			t, err := parseTime(nullString.String)
			if err != nil {
				return err
			}
			*ppts[i] = &t
		}
	}

	return err
}

func (c cursor) GetMap() (result map[string]value, err error) {
	columns, err := c.rows.Columns()
	if err != nil {
		return
	}

	columnCount := len(columns)
	values := make([]interface{}, columnCount)
	for i := 0; i < columnCount; i++ {
		var value *string
		values[i] = &value
	}
	if err = c.rows.Scan(values...); err != nil {
		return
	}

	result = make(map[string]value, columnCount)
	for i, column := range columns {
		result[column] = value{stringValue: *values[i].(**string)}
	}

	return
}

func (c cursor) Close() error {
	return c.rows.Close()
}
