package backend

import (
	"context"
	"net/url"

	"github.com/agnosticeng/objstr/types"
)

type Backend interface {
	ListPrefix(context.Context, *url.URL, ...types.ListOption) ([]*types.Object, error)
	ReadMetadata(context.Context, *url.URL) (*types.ObjectMetadata, error)
	Reader(context.Context, *url.URL) (types.Reader, error)
	ReaderAt(context.Context, *url.URL) (types.ReaderAt, error)
	Writer(context.Context, *url.URL) (types.Writer, error)
	Delete(context.Context, *url.URL) error
	Close() error
}

type MoveableBackend interface {
	Backend
	Move(context.Context, *url.URL, *url.URL) error
}
