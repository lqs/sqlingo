package sqlingo

import (
	"database/sql/driver"
	"io"
	"strconv"
	"testing"
)

type mockDriver struct{}

type mockConn struct {
	lastSql      string
	mockTx       *mockTx
	beginTxError error
	prepareError error
	columnCount  int
	rowCount     int
}

type mockStmt struct {
	columnCount int
	rowCount    int
}

type mockRows struct {
	columnCount    int
	cursorPosition int
	rowCount       int
}

func (m mockRows) Columns() []string {
	return []string{"a", "b", "c", "d", "e", "f", "g"}[:m.columnCount]
}

func (m mockRows) Close() error {
	return nil
}

func (m *mockRows) Next(dest []driver.Value) error {
	if m.cursorPosition >= m.rowCount {
		return io.EOF
	}
	m.cursorPosition++
	for i := 0; i < m.columnCount; i++ {
		switch i {
		case 0:
			dest[i] = strconv.Itoa(m.cursorPosition)
		case 1:
			dest[i] = float32(m.cursorPosition)
		case 2:
			dest[i] = m.cursorPosition
		case 3:
			dest[i] = string(rune(m.cursorPosition % 2)) // '\x00' or '\x01'
		case 4:
			dest[i] = strconv.Itoa(m.cursorPosition % 2) // '0' or '1'
		case 5:
			dest[i] = dest[0]
		case 6:
			dest[i] = nil
		}
	}
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
	return &mockRows{
		columnCount: m.columnCount,
		rowCount:    m.rowCount,
	}, nil
}

func TestCursor(t *testing.T) {
	db := newMockDatabase()
	cursor, _ := db.Query("dummy sql")

	var a int
	var b string

	var cde struct {
		C  float32
		DE struct {
			D, E bool
		}
	}
	var f ****int // deep pointer
	var g *int    // always null

	for i := 1; i <= 10; i++ {
		if !cursor.Next() {
			t.Error()
		}
		g = &i
		if err := cursor.Scan(&a, &b, &cde, &f, &g); err != nil {
			t.Errorf("%v", err)
		}
		if a != i ||
			b != strconv.Itoa(i) ||
			cde.C != float32(i) ||
			cde.DE.D != (i%2 == 1) ||
			cde.DE.E != cde.DE.D ||
			****f != i ||
			g != nil {
			t.Error(a, b, cde.C, cde.DE.D, cde.DE.E, ****f, g)
		}
		if err := cursor.Scan(); err != nil {
			t.Errorf("%v", err)
		}

		var s string
		var b ****bool
		var p *string
		var bs []byte
		if err := cursor.Scan(&s, &s, &s, &b, &s, &bs, &p); err != nil {
			t.Error(err)
		}
		if ****b != (i%2 == 1) ||
			p != nil ||
			string(bs) != strconv.Itoa(i) {
			t.Error(s, ****b, p, string(bs))
		}
	}
	if cursor.Next() {
		t.Errorf("d")
	}
	if err := cursor.Close(); err != nil {
		t.Error(err)
	}

}

func TestCursorMap(t *testing.T) {
	db := newMockDatabase()
	cursor, _ := db.Query("dummy sql")

	for i := 1; i <= 10; i++ {
		if !cursor.Next() {
			t.Error()
		}
		row, err := cursor.GetMap()
		if err != nil {
			t.Error(err)
		}
		if row["a"].Int() != i {
			t.Error()
		}
	}
	if cursor.Next() {
		t.Error()
	}
}
