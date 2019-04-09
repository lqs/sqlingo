package sqlingo

import (
	"context"
)

type InvokerFunc = func(ctx context.Context, sql string) error
type InterceptorFunc = func(ctx context.Context, sql string, invoker InvokerFunc) error

// TODO: add some common interceptors
