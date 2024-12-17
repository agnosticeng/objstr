package sftp

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/agnosticeng/objstr/types"
	slogctx "github.com/veqryn/slog-context"
)

type SFTPBackendConfig struct{}

type SFTPBackend struct {
	logger      *slog.Logger
	clientCache *ClientCache
}

func NewSFTPBackend(ctx context.Context, conf SFTPBackendConfig) *SFTPBackend {
	return &SFTPBackend{
		logger:      slogctx.FromCtx(ctx),
		clientCache: NewClientCache(),
	}
}

func (be *SFTPBackend) createClient(ctx context.Context, u *url.URL) (*Client, error) {
	client, err := NewClient(ctx, u.String())

	if err != nil {
		return nil, err
	}

	return client, nil
}

func (be *SFTPBackend) ListPrefix(ctx context.Context, u *url.URL, optFunc ...types.ListOption) ([]*types.Object, error) {
	var opts = types.NewListOptions(optFunc...)

	client, err := be.clientCache.Get(ctx, u)

	if err != nil {
		return nil, fmt.Errorf("failed to get client for %s: %w", u.String(), err)
	}

	var (
		res []*types.Object
		w   = client.SFTPClient().Walk(u.Path)
	)

	for w.Step() {
		if err := w.Err(); err != nil {
			be.logger.Info(err.Error())
			continue
		}

		if w.Stat().IsDir() {
			continue
		}

		newU, err := url.Parse(u.String())

		if err != nil {
			be.logger.Info(err.Error())
			continue
		}

		newU.Path = w.Path()

		if len(opts.StartAfter) > 0 {
			if strings.Compare(newU.String(), opts.StartAfter) < 0 {
				continue
			}
		}

		res = append(res, &types.Object{
			URL: newU,
			Metadata: &types.ObjectMetadata{
				Size:             uint64(w.Stat().Size()),
				ModificationDate: w.Stat().ModTime(),
			},
		})
	}

	return res, nil
}

func (be *SFTPBackend) ReadMetadata(ctx context.Context, u *url.URL) (*types.ObjectMetadata, error) {
	client, err := be.clientCache.Get(ctx, u)

	if err != nil {
		return nil, fmt.Errorf("failed to get client for %s: %w", u.String(), err)
	}

	stat, err := client.SFTPClient().Stat(filepath.Join(u.Host, u.Path))

	if err != nil {
		return nil, err
	}

	return &types.ObjectMetadata{
		Size:             uint64(stat.Size()),
		ModificationDate: stat.ModTime(),
	}, nil
}

func (be *SFTPBackend) Reader(ctx context.Context, u *url.URL) (types.Reader, error) {
	client, err := be.clientCache.Get(ctx, u)

	if err != nil {
		return nil, fmt.Errorf("failed to get client for %s: %w", u.String(), err)
	}

	return client.SFTPClient().Open(u.Path)
}

func (be *SFTPBackend) ReaderAt(ctx context.Context, u *url.URL) (types.ReaderAt, error) {
	client, err := be.clientCache.Get(ctx, u)

	if err != nil {
		return nil, fmt.Errorf("failed to get client for %s: %w", u.String(), err)
	}

	return client.SFTPClient().Open(u.Path)
}

func (be *SFTPBackend) Writer(ctx context.Context, u *url.URL) (types.Writer, error) {
	client, err := be.clientCache.Get(ctx, u)

	if err != nil {
		return nil, fmt.Errorf("failed to get client for %s: %w", u.String(), err)
	}

	if err := client.SFTPClient().MkdirAll(filepath.Dir(u.Path)); err != nil {
		return nil, err
	}

	return client.SFTPClient().Create(u.Path)
}

func (be *SFTPBackend) Delete(ctx context.Context, u *url.URL) error {
	client, err := be.clientCache.Get(ctx, u)

	if err != nil {
		return fmt.Errorf("failed to get client for %s: %w", u.String(), err)
	}

	return client.SFTPClient().Remove(u.Path)
}

func (be *SFTPBackend) Close() error {
	return be.clientCache.Close()
}
