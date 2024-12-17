package redis

import (
	"context"
	stderr "errors"
	"io"
	"log/slog"
	"net/url"
	"sync"

	"github.com/agnosticeng/objstr/errors"
	"github.com/agnosticeng/objstr/types"
	"github.com/redis/rueidis"
	slogctx "github.com/veqryn/slog-context"
)

type RedisBackendConfig struct {
	Dsn string
}

type RedisBackend struct {
	conf       RedisBackendConfig
	logger     *slog.Logger
	clientLock sync.Mutex
	client     rueidis.Client
	clientOpts rueidis.ClientOption
}

func NewRedisBackend(ctx context.Context, conf RedisBackendConfig) (*RedisBackend, error) {
	opts, err := rueidis.ParseURL(conf.Dsn)

	if err != nil {
		return nil, err
	}

	return &RedisBackend{
		conf:       conf,
		logger:     slogctx.FromCtx(ctx),
		clientOpts: opts,
	}, nil
}

func (be *RedisBackend) getClient(ctx context.Context) (rueidis.Client, error) {
	be.clientLock.Lock()
	defer be.clientLock.Unlock()

	if be.client != nil {
		return be.client, nil
	}

	client, err := rueidis.NewClient(be.clientOpts)

	if err != nil {
		return nil, err
	}

	be.client = client
	return client, err
}

func (be *RedisBackend) ListPrefix(ctx context.Context, u *url.URL, optFunc ...types.ListOption) ([]*types.Object, error) {
	return nil, stderr.ErrUnsupported
}

func (be *RedisBackend) ReadMetadata(ctx context.Context, u *url.URL) (*types.ObjectMetadata, error) {
	return nil, stderr.ErrUnsupported
}

func (be *RedisBackend) Reader(ctx context.Context, u *url.URL) (types.Reader, error) {
	client, err := be.getClient(ctx)

	if err != nil {
		return nil, err
	}

	var key = u.Hostname() + u.Path

	r, err := client.Do(ctx, be.client.B().Get().Key(key).Build()).AsReader()

	if stderr.Is(err, rueidis.Nil) {
		return nil, errors.ErrObjectNotFound
	}

	return io.NopCloser(r), nil
}

func (be *RedisBackend) ReaderAt(ctx context.Context, u *url.URL) (types.ReaderAt, error) {
	return nil, stderr.ErrUnsupported
}

func (be *RedisBackend) Writer(ctx context.Context, u *url.URL) (types.Writer, error) {
	var key = u.Hostname() + u.Path

	client, err := be.getClient(ctx)

	if err != nil {
		return nil, err
	}

	return NewRedisWriter(client, key), nil
}

func (be *RedisBackend) Delete(ctx context.Context, u *url.URL) error {
	client, err := be.getClient(ctx)

	if err != nil {
		return err
	}

	var key = u.Hostname() + u.Path
	return client.Do(ctx, be.client.B().Del().Key(key).Build()).Error()
}

func (be *RedisBackend) Close() error {
	if be.client != nil {
		be.client.Close()
	}

	return nil
}
