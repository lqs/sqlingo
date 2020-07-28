package main

import (
	"fmt"
	_ "github.com/lib/pq"
	"github.com/lqs/sqlingo/generator"
)

func main() {
	code, err := generator.Generate("postgres", "host=localhost port=5432 user=user password=pass dbname=db sslmode=disable")
	if err != nil {
		panic(err)
	}

	fmt.Print(code)
}
