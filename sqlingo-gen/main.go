package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"strings"
)

func printUsageAndExit() {
	cmd := os.Args[0]
	_, _ = fmt.Fprintf(os.Stderr, `Usage:
	%s [-t table1,table2,...] [driverName] dataSourceName
Example:
	%s mysql "username:password@tcp(hostname:3306)/database"
	%s sqlite3 ./testdb.sqlite3
`, cmd, cmd, cmd)
	os.Exit(1)
}

func parseArgs() (driverName string, dataSourceName string, tableNames []string) {
	var args []string
	parseTable := false
	for _, arg := range os.Args[1:] {
		if arg != "" && arg[0] == '-' {
			switch arg[1:] {
			case "t":
				if parseTable {
					printUsageAndExit()
				}
				parseTable = true
			default:
				printUsageAndExit()
			}
		} else {
			if parseTable {
				tableNames = append(tableNames, strings.Split(arg, ",")...)
				parseTable = false
			} else {
				args = append(args, arg)
			}
		}
	}
	if parseTable {
		// "-t" not closed
		printUsageAndExit()
	}

	switch len(args) {
	case 1:
		driverName = "mysql"
		dataSourceName = args[0]
	case 2:
		driverName = args[0]
		dataSourceName = args[1]
	default:
		printUsageAndExit()
	}

	return
}

func main() {
	driverName, dataSourceName, tableNames := parseArgs()
	code, err := generate(driverName, dataSourceName, tableNames)
	if err != nil {
		panic(err)
	}

	fmt.Print(code)
}
