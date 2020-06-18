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

func Function(name string, args ...interface{}) Expression {
	return function(name, args...)
}

func Concat(args ...interface{}) StringExpression {
	return function("CONCAT", args...)
}

func Count(arg interface{}) NumberExpression {
	return function("COUNT", arg)
}

func If(predicate Expression, trueValue interface{}, falseValue interface{}) (result Expression) {
	return function("IF", predicate, trueValue, falseValue)
}

func Length(arg interface{}) NumberExpression {
	return function("LENGTH", arg)
}

func Sum(arg interface{}) NumberExpression {
	return function("SUM", arg)
}
