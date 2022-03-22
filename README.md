<img src="https://raw.githubusercontent.com/lqs/sqlingo/master/logo.png" width="236" height="106">

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)
[![go.dev](https://img.shields.io/badge/go.dev-reference-007d9c)](https://pkg.go.dev/github.com/lqs/sqlingo?tab=doc)
[![Travis CI](https://api.travis-ci.com/lqs/sqlingo.svg?branch=master)](https://app.travis-ci.com/github/lqs/sqlingo)
[![Go Report Card](https://goreportcard.com/badge/github.com/lqs/sqlingo)](https://goreportcard.com/report/github.com/lqs/sqlingo)
[![codecov](https://codecov.io/gh/lqs/sqlingo/branch/master/graph/badge.svg)](https://codecov.io/gh/lqs/sqlingo)
[![MIT license](http://img.shields.io/badge/license-MIT-9d1f14)](http://opensource.org/licenses/MIT)
[![last commit](https://img.shields.io/github/last-commit/lqs/sqlingo.svg)](https://github.com/lqs/sqlingo/commits)

**sqlingo** is a SQL DSL (a.k.a. SQL Builder or ORM) library in Go. It generates code from the database and lets you write SQL queries in an elegant way.

<img src="https://lqs-public-us-west.oss-us-west-1.aliyuncs.com/sqlingo/demo2.gif" width="443" height="297">

## Features
* Auto-generating DSL objects and model structs from the database so you don't need to manually keep things in sync
* SQL DML (SELECT / INSERT / UPDATE / DELETE) with some advanced SQL query syntaxes
* Many common errors could be detected at compile time
* Your can use the features in your editor / IDE, such as autocompleting the fields and queries, or finding the usage of a field or a table
* Context support
* Transaction support
* Interceptor support

## Database Support Status
| Database    | Status       |
------------- | --------------
| MySQL       | stable       |
| PostgreSQL  | experimental |
| SQLite      | experimental |

## Tutorial

### Install and use sqlingo code generator
The first step is to generate code from the database. In order to generate code, sqlingo requires your tables are already created in the database.

```
$ go get -u github.com/lqs/sqlingo/sqlingo-gen-mysql
$ mkdir -p generated/sqlingo
$ sqlingo-gen-mysql root:123456@/database_name >generated/sqlingo/database_name.dsl.go
```


### Write your application
Here's a demonstration of some simple & advanced usage of sqlingo.
```go
package main

import (
    "github.com/lqs/sqlingo"
    . "./generated/sqlingo"
)

func main() {
    db, err := sqlingo.Open("mysql", "root:123456@/database_name")
    if err != nil {
        panic(err)
    }

    // a simple query
    var customers []*CustomerModel
    db.SelectFrom(Customer).
        Where(Customer.Id.In(1, 2)).
    	OrderBy(Customer.Name.Desc()).
        FetchAll(&customers)

    // query from multiple tables
    var customerId int64
    var orderId int64
    err = db.Select(Customer.Id, Order.Id).
        From(Customer, Order).
        Where(Customer.Id.Equals(Order.CustomerId), Order.Id.Equals(1)).
        FetchFirst(&customerId, &orderId)
    
    // subquery and count
    count, err := db.SelectFrom(Order)
        Where(Order.CustomerId.In(db.Select(Customer.Id).
            From(Customer).
            Where(Customer.Name.Equals("Customer One")))).
    	Count()
        
    // group-by with auto conversion to map
    var customerIdToOrderCount map[int64]int64
    err = db.Select(Order.CustomerId, f.Count(1)).
    	From(Order).
    	GroupBy(Order.CustomerId).
    	FetchAll(&customerIdToOrderCount)
    if err != nil {
    	println(err)
    }
    
    // insert some rows
    customer1 := &CustomerModel{name: "Customer One"}
    customer2 := &CustomerModel{name: "Customer Two"}
    _, err = db.InsertInto(Customer).
        Models(customer1, customer2).
        Execute()
    
    // insert with on-duplicate-key-update
    _, err = db.InsertInto(Customer).
    	Fields(Customer.Id, Customer.Name).
    	Values(42, "Universe").
    	OnDuplicateKeyUpdate().
    	Set(Customer.Name, Customer.Name.Concat(" 2")).
    	Execute()
}
```
