<img src="https://raw.githubusercontent.com/lqs/sqlingo/master/logo.png" width="236" height="106">

**sqlingo** is a SQL DSL & ORM library in Go. It generates code from your database and lets you write SQL queries easily.

WARNING: sqlingo is still under development. It's expected to be released by end of November 2018.

## Tutorial

### Prepare your database
In order to generate code, sqlingo requires your tables are already created in the database.

### Install sqlingo code generator
```
$ go get -u github.com/lqs/sqlingo/sqlingo-gen
```

### Generate code for your database
```
$ mkdir -p generated/sqlingo
$ sqlingo-gen root:123456@/database_name >generated/sqlingo/database_name.dsl.go
```

### Start using sqlingo
Create `main.go` to use the generated code
```go
import (
    "github.com/lqs/sqlingo"
    "./generated/sqlingo"
)

func main() {
    db, err := sqlingo.Open("mysql", "root:123456@/database_name")
    if err != nil {
        panic(err)
    }
    
    # insert some rows
    customer1 := &CustomerModel{name: "Customer One"}
    customer2 := &CustomerModel{name: "Customer Two"}
    db.InsertInto(Customer).
        Values(customer1, customer2).
        Execute()

    # do some queries
    var customers []*CustomerModel
    db.SelectFrom(Customer).
        Where(Customer.Id.In(1, 2)).
        Fetch(&customers)

    # more examples
    var customerId int64
    var orderId int64
    db.Select(Customer.Id, Order.Id).
        From(Customer, Order).
        Where(Customer.Id.Equals(Order.CustomerId), Order.Id.Equals(1)).
        Fetch(&customerId, &orderId)
}
```
