package main

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"sqlingo/generator"
)

func main() {
	code, err := generator.Generate("mysql", "./testdb.sqlite3")
	if err != nil {
		panic(err)
	}

	fmt.Print(code)
}
