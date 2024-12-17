package memory

import (
	"context"
	stderr "errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/agnosticeng/objstr/errors"
	"github.com/agnosticeng/objstr/types"
	"github.com/spf13/afero"
)

type MemoryBackendConfig struct{}

type MemoryBackend struct {
	fs afero.Fs
}

func NewMemoryBackend(ctx context.Context, conf MemoryBackendConfig) *MemoryBackend {
	return &MemoryBackend{
		fs: afero.NewMemMapFs(),
	}
}

func (be *MemoryBackend) validateURL(u *url.URL) error {
	if len(u.Path) == 0 && len(u.Host) == 0 {
		return fmt.Errorf("path and host can't both be empty")
	}

	return nil
}

func (be *MemoryBackend) ListPrefix(ctx context.Context, u *url.URL, optFunc ...types.ListOption) ([]*types.Object, error) {
	return nil, stderr.ErrUnsupported
}

func (be *MemoryBackend) ReadMetadata(ctx context.Context, u *url.URL) (*types.ObjectMetadata, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	stat, err := be.fs.Stat(filepath.Join(u.Host, u.Path))

	if os.IsNotExist(err) {
		return nil, errors.ErrObjectNotFound
	}

	if err != nil {
		return nil, err
	}

	return &types.ObjectMetadata{
		Size: uint64(stat.Size()),
	}, nil
}

func (be *MemoryBackend) Reader(ctx context.Context, u *url.URL) (types.Reader, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	f, err := be.fs.Open(filepath.Join(u.Host, u.Path))

	if os.IsNotExist(err) {
		return nil, errors.ErrObjectNotFound
	}

	return f, nil
}

func (be *MemoryBackend) ReaderAt(ctx context.Context, u *url.URL) (types.ReaderAt, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	f, err := be.fs.Open(filepath.Join(u.Host, u.Path))

	if os.IsNotExist(err) {
		return nil, errors.ErrObjectNotFound
	}

	return f, nil
}

func (be *MemoryBackend) Writer(ctx context.Context, u *url.URL) (types.Writer, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	path := filepath.Join(u.Host, u.Path)

	if err := be.fs.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return nil, err
	}

	return be.fs.Create(path)
}

func (be *MemoryBackend) Delete(ctx context.Context, u *url.URL) error {
	if err := be.validateURL(u); err != nil {
		return err
	}

	return be.fs.Remove(filepath.Join(u.Host, u.Path))
}

func (be *MemoryBackend) Close() error {
	return nil
}
