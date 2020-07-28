package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lqs/sqlingo/generator"
)

func main() {
	code, err := generator.Generate("mysql", "username:password@tcp(hostname:3306)/database")
	if err != nil {
		panic(err)
	}

	fmt.Print(code)
}
