package sqlingo

import (
	"testing"
)

func TestSelect(t *testing.T) {
	db := database{}
	assertValue(t, db.Select(1), "(SELECT 1)")
}