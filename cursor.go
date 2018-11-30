package sqlingo

import (
	"database/sql"
	"fmt"
	"reflect"
)

type Cursor interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
}

type cursor struct {
	rows *sql.Rows
}

func (c cursor) Next() bool {
	return c.rows.Next()
}

func preparePointers(val reflect.Value, scans *[]interface{}) error {
	kind := val.Kind()
	switch kind {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		*scans = append(*scans, val.Addr().Interface())
	case reflect.Slice:
	case reflect.Struct:
		for j := 0; j < val.NumField(); j++ {
			field := val.Field(j)
			if field.Kind() == reflect.Interface {
				continue
			}
			*scans = append(*scans, field.Addr().Interface())
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
		default:
			to := reflect.New(toType).Elem()
			val.Set(to.Addr())
			err := preparePointers(to, scans)
			if err != nil {
				return nil
			}
		}
	default:
		return fmt.Errorf("unknown type %s", kind.String())
	}
	return nil
}

func (c cursor) Scan(dest ...interface{}) error {

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

	err := c.rows.Scan(scans...)
	return err
}

func (c cursor) Close() error {
	return c.rows.Close()
}
