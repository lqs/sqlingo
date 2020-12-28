package generator

import (
	"fmt"
	"os"
	"strings"
)

func printUsageAndExit(exampleDataSourceName string) {
	cmd := os.Args[0]
	_, _ = fmt.Fprintf(os.Stderr, `Usage:
	%s [-t table1,table2,...] dataSourceName
Example:
	%s "%s"
`, cmd, cmd, exampleDataSourceName)
	os.Exit(1)
}

func parseArgs(exampleDataSourceName string) (dataSourceName string, tableNames []string) {
	var args []string
	parseTable := false
	for _, arg := range os.Args[1:] {
		if arg != "" && arg[0] == '-' {
			switch arg[1:] {
			case "t":
				if parseTable {
					printUsageAndExit(exampleDataSourceName)
				}
				parseTable = true
			default:
				printUsageAndExit(exampleDataSourceName)
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
		printUsageAndExit(exampleDataSourceName)
	}

	switch len(args) {
	case 1:
		dataSourceName = args[0]
	default:
		printUsageAndExit(exampleDataSourceName)
	}

	return
}
