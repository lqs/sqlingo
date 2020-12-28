package main

import (
	"fmt"
	"github.com/lqs/sqlingo/generator"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	code, err := generator.Generate("mysql", "./testdb.sqlite3")
	if err != nil {
		panic(err)
	}

	fmt.Print(code)
}
