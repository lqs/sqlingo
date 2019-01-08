package main

import (
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strings"
)

func main() {

	t := flag.String("t", "", "")
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		fmt.Printf("Usage: %s [-t table1,table2,...] username:password@/database\n", os.Args[0])
		return
	}
	dataSourceName := args[0]

	tableNames := strings.Split(*t, ",")
	if len(tableNames) == 1 && tableNames[0] == "" {
		tableNames = nil
	}

	code, err := generate("mysql", dataSourceName, tableNames)
	if err != nil {
		panic(err)
	}

	fmt.Print(code)
}
