package sqlingo

import (
	"context"
)

// InvokerFunc is the function type of the actual invoker. It should be called in an interceptor.
type InvokerFunc = func(ctx context.Context, sql string) error

// InterceptorFunc is the function type of an interceptor. An interceptor should implement this function to fulfill it's purpose.
type InterceptorFunc = func(ctx context.Context, sql string, invoker InvokerFunc) error

// TODO: add some common interceptors
