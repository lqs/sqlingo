package sqlingo

import (
	"context"
)

// InvokerFunc is the function type of the actual invoker. It should be called in an interceptor.
type InvokerFunc = func(ctx context.Context, sql string) error

// InterceptorFunc is the function type of an interceptor. An interceptor should implement this function to fulfill it's purpose.
type InterceptorFunc = func(ctx context.Context, sql string, invoker InvokerFunc) error

func noopInterceptor(ctx context.Context, sql string, invoker InvokerFunc) error {
	return invoker(ctx, sql)
}

// ChainInterceptors chains multiple interceptors into one interceptor.
func ChainInterceptors(interceptors ...InterceptorFunc) InterceptorFunc {
	if len(interceptors) == 0 {
		return noopInterceptor
	}
	return func(ctx context.Context, sql string, invoker InvokerFunc) error {
		var chain func(int, context.Context, string) error
		chain = func(i int, ctx context.Context, sql string) error {
			if i == len(interceptors) {
				return invoker(ctx, sql)
			}
			return interceptors[i](ctx, sql, func(ctx context.Context, sql string) error {
				return chain(i+1, ctx, sql)
			})
		}
		return chain(0, ctx, sql)
	}
}
