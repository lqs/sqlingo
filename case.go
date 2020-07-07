package sqlingo

import "strings"

// CaseExpression indicates the status in a CASE statement
type CaseExpression interface {
	WhenThen(when BooleanExpression, then interface{}) CaseExpression
	Else(value interface{}) CaseExpressionWithElse
	End() Expression
}

// CaseExpressionWithElse indicates the status in CASE ... ELSE ... statement
type CaseExpressionWithElse interface {
	End() Expression
}

type caseStatus struct {
	head, tail *whenThen
	elseValue  interface{}
}

type whenThen struct {
	next *whenThen
	when BooleanExpression
	then interface{}
}

// Case initiates a CASE statement
func Case() CaseExpression {
	return caseStatus{}
}

func (s caseStatus) WhenThen(when BooleanExpression, then interface{}) CaseExpression {
	whenThen := &whenThen{when: when, then: then}
	if s.head == nil {
		s.head = whenThen
	}
	if s.tail != nil {
		s.tail.next = whenThen
	}
	s.tail = whenThen
	return s
}

func (s caseStatus) Else(value interface{}) CaseExpressionWithElse {
	s.elseValue = value
	return s
}

func (s caseStatus) End() Expression {
	if s.head == nil {
		return expression{
			builder: func(scope scope) (string, error) {
				elseSql, _, err := getSQL(scope, s.elseValue)
				return elseSql, err
			},
		}
	}

	return expression{
		builder: func(scope scope) (string, error) {
			sb := strings.Builder{}
			sb.WriteString("CASE ")

			for whenThen := s.head; whenThen != nil; whenThen = whenThen.next {
				whenSql, err := whenThen.when.GetSQL(scope)
				if err != nil {
					return "", err
				}
				thenSql, _, err := getSQL(scope, whenThen.then)
				if err != nil {
					return "", err
				}
				sb.WriteString("WHEN " + whenSql + " THEN " + thenSql + " ")
			}
			if s.elseValue != nil {
				elseSql, _, err := getSQL(scope, s.elseValue)
				if err != nil {
					return "", err
				}
				sb.WriteString("ELSE " + elseSql + " ")
			}
			sb.WriteString("END")
			return sb.String(), nil
		},
	}
}
