package sqlingo

import (
	"context"
	"testing"
)

func TestChainInterceptors(t *testing.T) {
	s := ""
	i1 := func(ctx context.Context, sql string, invoker InvokerFunc) error {
		s += "<i1>"
		s += sql
		defer func() {
			s += "</i1>"
		}()
		return invoker(ctx, sql+"s1")
	}
	i2 := func(ctx context.Context, sql string, invoker InvokerFunc) error {
		s += "<i2>"
		s += sql
		defer func() {
			s += "</i2>"
		}()
		return invoker(ctx, sql+"s2")
	}
	chain := ChainInterceptors(i1, i2)
	_ = chain(context.Background(), "sql", func(ctx context.Context, sql string) error {
		s += "<invoker>"
		s += sql
		defer func() {
			s += "</invoker>"
		}()
		return nil
	})
	if s != "<i1>sql<i2>sqls1<invoker>sqls1s2</invoker></i2></i1>" {
		t.Error(s)
	}
}

func TestEmptyChainInterceptors(t *testing.T) {
	s := ""
	chain := ChainInterceptors()
	_ = chain(context.Background(), "sql", func(ctx context.Context, sql string) error {
		s += "<invoker>"
		defer func() {
			s += "</invoker>"
		}()
		s += sql
		return nil
	})

	if s != "<invoker>sql</invoker>" {
		t.Error(s)
	}
}
