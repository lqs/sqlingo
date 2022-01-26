package sqlingo

func function(name string, args ...interface{}) expression {
	return expression{builder: func(scope scope) (string, error) {
		valuesSql, err := commaValues(scope, args)
		if err != nil {
			return "", err
		}
		return name + "(" + valuesSql + ")", nil
	}}
}

// Function creates an expression of the call to specified function.
func Function(name string, args ...interface{}) Expression {
	return function(name, args...)
}

// Concat creates an expression of CONCAT function.
func Concat(args ...interface{}) StringExpression {
	return function("CONCAT", args...)
}

// Count creates an expression of COUNT aggregator.
func Count(arg interface{}) NumberExpression {
	return function("COUNT", arg)
}

// If creates an expression of IF function.
func If(predicate Expression, trueValue interface{}, falseValue interface{}) (result UnknownExpression) {
	return function("IF", predicate, trueValue, falseValue)
}

// Length creates an expression of LENGTH function.
func Length(arg interface{}) NumberExpression {
	return function("LENGTH", arg)
}

// Sum creates an expression of SUM aggregator.
func Sum(arg interface{}) NumberExpression {
	return function("SUM", arg)
}
