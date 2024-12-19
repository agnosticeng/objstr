package objstr

import (
	"context"
)

type contextKey struct{}

func NewContext(ctx context.Context, os *ObjectStore) context.Context {
	return context.WithValue(ctx, contextKey{}, os)
}

func FromContext(ctx context.Context) *ObjectStore {
	return ctx.Value(contextKey{}).(*ObjectStore)
}

func FromContextOrDefault(ctx context.Context) *ObjectStore {
	v, ok := ctx.Value(contextKey{}).(*ObjectStore)

	if !ok {
		return GetDefault()
	}

	return v
}
