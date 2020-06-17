package sqlingo

import (
	"database/sql/driver"
	"io"
	"strconv"
	"testing"
)

type mockDriver struct{}

type mockConn struct {
	lastSql string
	mockTx  *mockTx
}

type mockStmt struct{}

type mockRows struct {
	count int
}

func (m mockRows) Columns() []string {
	return []string{"a", "b", "c", "d", "e", "f", "g"}
}

func (m mockRows) Close() error {
	return nil
}

func (m *mockRows) Next(dest []driver.Value) error {
	if m.count >= 10 {
		return io.EOF
	}
	m.count++
	dest[0] = strconv.Itoa(m.count)
	dest[1] = float32(m.count)
	dest[2] = m.count
	dest[3] = string(m.count % 2)       // '\x00' or '\x01'
	dest[4] = strconv.Itoa(m.count % 2) // '0' or '1'
	dest[5] = dest[0]
	dest[6] = nil
	return nil
}

func (m mockStmt) Close() error {
	return nil
}

func (m mockStmt) NumInput() int {
	return 0
}

func (m mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return driver.ResultNoRows, nil
}

func (m mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &mockRows{}, nil
}

func TestCursor(t *testing.T) {
	db := newMockDatabase()
	cursor, _ := db.Query("dummy sql")

	var a int
	var b string

	var cde struct {
		C    float32
		D, E bool
	}
	var f ****int // deep pointer
	var g *int    // always null

	for i := 1; i <= 10; i++ {
		if !cursor.Next() {
			t.Errorf("a")
		}
		g = &i
		if err := cursor.Scan(&a, &b, &cde, &f, &g); err != nil {
			t.Errorf("%v", err)
		}
		if a != i ||
			b != strconv.Itoa(i) ||
			cde.C != float32(i) ||
			cde.D != (i%2 == 1) ||
			cde.E != cde.E ||
			****f != i ||
			g != nil {
			t.Error(a, b, cde.C, cde.D, cde.E, ****f, g)
		}
		if err := cursor.Scan(); err != nil {
			t.Errorf("%v", err)
		}

		var s string
		var b ****bool
		var p *string
		if err := cursor.Scan(&s, &s, &s, &b, &s, &s, &p); err != nil {
			t.Error(err)
		}
	}
	if cursor.Next() {
		t.Errorf("d")
	}
	if err := cursor.Close(); err != nil {
		t.Error(err)
	}

}
