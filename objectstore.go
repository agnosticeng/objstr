package objstr

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/agnosticeng/objstr/backend"
	"github.com/agnosticeng/objstr/backend/impl/fs"
	"github.com/agnosticeng/objstr/backend/impl/http"
	"github.com/agnosticeng/objstr/backend/impl/memory"
	"github.com/agnosticeng/objstr/backend/impl/redis"
	"github.com/agnosticeng/objstr/backend/impl/s3"
	"github.com/agnosticeng/objstr/backend/impl/sftp"
	"github.com/agnosticeng/objstr/types"
	"github.com/hashicorp/go-multierror"
)

type ObjectStore struct {
	conf     ObjectStoreConfig
	backends map[string]backend.Backend
}

func NewObjectStore(ctx context.Context, conf ObjectStoreConfig) (*ObjectStore, error) {
	if conf.CopyBufferSize == 0 {
		conf.CopyBufferSize = 1024 * 1024
	}

	var backends = make(map[string]backend.Backend)

	if !conf.DisableDefaultBackends {
		var (
			fsBackend   = fs.NewFSBackend(ctx, fs.FSBackendConfig{})
			httpBackend = http.NewHTTPBackend(ctx, http.HTTPBackendConfig{})
			sftpBackend = sftp.NewSFTPBackend(ctx, sftp.SFTPBackendConfig{})
			memBackend  = memory.NewMemoryBackend(ctx, memory.MemoryBackendConfig{})
		)

		backends[""] = fsBackend
		backends["file"] = fsBackend
		backends["mem"] = memBackend
		backends["memory"] = memBackend
		backends["http"] = httpBackend
		backends["https"] = httpBackend
		backends["sftp"] = sftpBackend
	}

	for _, backendConf := range conf.Backends {
		var (
			backend backend.Backend
			err     error
		)

		switch {
		case backendConf.Fs != nil:
			backend = fs.NewFSBackend(ctx, *backendConf.Fs)
		case backendConf.Mem != nil:
			backend = memory.NewMemoryBackend(ctx, *backendConf.Mem)
		case backendConf.Http != nil:
			backend = http.NewHTTPBackend(ctx, *backendConf.Http)
		case backendConf.S3 != nil:
			backend, err = s3.NewS3Backend(ctx, *backendConf.S3)
		case backendConf.Redis != nil:
			backend, err = redis.NewRedisBackend(ctx, *backendConf.Redis)
		default:
			return nil, fmt.Errorf("backend conf must be specified for scheme: %s", backendConf.Scheme)
		}

		if err != nil {
			return nil, err
		}

		backends[backendConf.Scheme] = backend
	}

	return &ObjectStore{
		conf:     conf,
		backends: backends,
	}, nil
}

func MustNewObjectStore(ctx context.Context, conf ObjectStoreConfig) *ObjectStore {
	if store, err := NewObjectStore(ctx, conf); err != nil {
		panic(err)
	} else {
		return store
	}
}

func (os *ObjectStore) getBackend(u *url.URL) (backend.Backend, error) {
	if u == nil {
		return nil, fmt.Errorf("url must not be nil")
	}

	backend, found := os.backends[u.Scheme]

	if !found {
		return nil, fmt.Errorf("no backend found for scheme %s", u.Scheme)
	}

	return backend, nil
}

func (os *ObjectStore) ListPrefix(ctx context.Context, u *url.URL, optsFunc ...types.ListOption) ([]*types.Object, error) {
	backend, err := os.getBackend(u)

	if err != nil {
		return nil, err
	}

	objects, err := backend.ListPrefix(ctx, u, optsFunc...)

	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		object.URL.Scheme = u.Scheme
	}

	return objects, nil
}

func (os *ObjectStore) ReadMetadata(ctx context.Context, u *url.URL) (*types.ObjectMetadata, error) {
	backend, err := os.getBackend(u)

	if err != nil {
		return nil, err
	}

	return backend.ReadMetadata(ctx, u)
}

func (os *ObjectStore) Reader(ctx context.Context, u *url.URL) (types.Reader, error) {
	backend, err := os.getBackend(u)

	if err != nil {
		return nil, err
	}

	return backend.Reader(ctx, u)
}

func (os *ObjectStore) ReaderAt(ctx context.Context, u *url.URL) (types.ReaderAt, error) {
	backend, err := os.getBackend(u)

	if err != nil {
		return nil, err
	}

	return backend.ReaderAt(ctx, u)
}

func (os *ObjectStore) Writer(ctx context.Context, u *url.URL) (types.Writer, error) {
	backend, err := os.getBackend(u)

	if err != nil {
		return nil, err
	}

	return backend.Writer(ctx, u)
}

func (os *ObjectStore) Delete(ctx context.Context, u *url.URL) error {
	backend, err := os.getBackend(u)

	if err != nil {
		return err
	}

	return backend.Delete(ctx, u)
}

func (os *ObjectStore) copy(ctx context.Context, srcBackend backend.Backend, dstBackend backend.Backend, src *url.URL, dst *url.URL) error {
	buf := make([]byte, os.conf.CopyBufferSize)

	srcReader, err := srcBackend.Reader(ctx, src)

	if err != nil {
		return err
	}

	defer srcReader.Close()

	dstWriter, err := dstBackend.Writer(ctx, dst)

	if err != nil {
		return err
	}

	if _, err := io.CopyBuffer(dstWriter, srcReader, buf); err != nil {
		return err
	}

	return dstWriter.Close()
}

func (os *ObjectStore) Copy(ctx context.Context, src *url.URL, dst *url.URL) error {
	srcBackend, err := os.getBackend(src)

	if err != nil {
		return err
	}

	dstBackend, err := os.getBackend(dst)

	if err != nil {
		return err
	}

	return os.copy(ctx, srcBackend, dstBackend, src, dst)
}

func (os *ObjectStore) Move(ctx context.Context, src *url.URL, dst *url.URL) error {
	srcBackend, err := os.getBackend(src)

	if err != nil {
		return err
	}

	dstBackend, err := os.getBackend(dst)

	if err != nil {
		return err
	}

	if srcBackend == dstBackend {
		if moveableBackend, ok := srcBackend.(backend.MoveableBackend); ok {
			return moveableBackend.Move(ctx, src, dst)
		}
	}

	if err := os.copy(ctx, srcBackend, dstBackend, src, dst); err != nil {
		return err
	}

	return srcBackend.Delete(ctx, src)
}

func (os *ObjectStore) Close() error {
	var res *multierror.Error

	for _, backend := range os.backends {
		if err := backend.Close(); err != nil {
			err = multierror.Append(res, err)
		}
	}

	return res.ErrorOrNil()
}
