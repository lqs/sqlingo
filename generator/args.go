package generator

import (
	"fmt"
	"os"
	"strings"
)

type options struct {
	dataSourceName string
	tableNames     []string
	forceCases     []string
}

func printUsageAndExit(exampleDataSourceName string) {
	cmd := os.Args[0]
	_, _ = fmt.Fprintf(os.Stderr, `Usage:
	%s [-t table1,table2,...] [-forcecases ID,IDs,HTML] dataSourceName
Example:
	%s "%s"
`, cmd, cmd, exampleDataSourceName)
	os.Exit(1)
}

func parseArgs(exampleDataSourceName string) (options options) {
	var args []string
	parseTable := false
	parseForceCases := false
	for _, arg := range os.Args[1:] {
		if arg != "" && arg[0] == '-' {
			switch arg[1:] {
			case "t":
				if parseTable {
					printUsageAndExit(exampleDataSourceName)
				}
				parseTable = true
			case "forcecases":
				if parseForceCases {
					printUsageAndExit(exampleDataSourceName)
				}
				parseForceCases = true
			case "timeAsString":
				timeAsString = true
			default:
				printUsageAndExit(exampleDataSourceName)
			}
		} else {
			if parseTable {
				options.tableNames = append(options.tableNames, strings.Split(arg, ",")...)
				parseTable = false
			} else if parseForceCases {
				options.forceCases = append(options.forceCases, strings.Split(arg, ",")...)
				parseForceCases = false
			} else {
				args = append(args, arg)
			}
		}
	}
	if parseTable || parseForceCases {
		// "-t" not closed
		printUsageAndExit(exampleDataSourceName)
	}

	switch len(args) {
	case 1:
		options.dataSourceName = args[0]
	default:
		printUsageAndExit(exampleDataSourceName)
	}

	return
}
