package sqlingo

type JsonExpression interface {
	JsonType() StringExpression
	JsonValid() BooleanExpression
	JsonDepth() NumberExpression
	JsonLength() NumberExpression
	JsonExtract(paths ...string) StringExpression
	JsonUnquote() UnknownExpression
	JsonMergePatch(others ...interface{}) StringExpression
	JsonMergePreserve(others ...interface{}) StringExpression
	JsonContainsPathOne(paths ...string) BooleanExpression
	JsonContainsPathAll(paths ...string) BooleanExpression
}

func (e expression) JsonType() StringExpression {
	return function("JSON_TYPE", e)
}

func (e expression) JsonValid() BooleanExpression {
	return function("JSON_VALID", e)
}

func (e expression) JsonDepth() NumberExpression {
	return function("JSON_DEPTH", e)
}

func (e expression) JsonLength() NumberExpression {
	return function("JSON_LENGTH", e)
}

func (e expression) JsonExtract(paths ...string) StringExpression {
	args := make([]interface{}, 0, 1+len(paths))
	args = append(args, e)
	for _, path := range paths {
		args = append(args, path)
	}
	return function("JSON_EXTRACT", args...)
}

func (e expression) JsonUnquote() UnknownExpression {
	return function("JSON_UNQUOTE", e)
}

func (e expression) JsonMergePatch(others ...interface{}) StringExpression {
	args := make([]interface{}, 0, 1+len(others))
	args = append(args, e)
	args = append(args, others...)
	return function("JSON_MERGE_PATCH", args...)
}

func (e expression) JsonMergePreserve(others ...interface{}) StringExpression {
	args := make([]interface{}, 0, 1+len(others))
	args = append(args, e)
	args = append(args, others...)
	return function("JSON_MERGE_PRESERVE", args...)
}

func (e expression) JsonContainsPathOne(paths ...string) BooleanExpression {
	args := make([]interface{}, 0, 2+len(paths))
	args = append(args, e)
	args = append(args, "one")
	for _, path := range paths {
		args = append(args, path)
	}
	return function("JSON_CONTAINS_PATH", args...)
}

func (e expression) JsonContainsPathAll(paths ...string) BooleanExpression {
	args := make([]interface{}, 0, 2+len(paths))
	args = append(args, e)
	args = append(args, "all")
	for _, path := range paths {
		args = append(args, path)
	}
	return function("JSON_CONTAINS_PATH", args...)
}
