package objstr

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"dario.cat/mergo"
	"github.com/agnosticeng/objstr/backend"
	"github.com/agnosticeng/objstr/backend/impl/fs"
	"github.com/agnosticeng/objstr/backend/impl/git"
	"github.com/agnosticeng/objstr/backend/impl/http"
	"github.com/agnosticeng/objstr/backend/impl/memory"
	"github.com/agnosticeng/objstr/backend/impl/redis"
	"github.com/agnosticeng/objstr/backend/impl/s3"
	"github.com/agnosticeng/objstr/backend/impl/sftp"
	"github.com/agnosticeng/objstr/types"
	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
)

type ObjectStore struct {
	conf     Config
	backends map[string]backend.Backend
}

func NewObjectStore(ctx context.Context, conf Config) (*ObjectStore, error) {
	if conf.CopyBufferSize == 0 {
		conf.CopyBufferSize = 1024 * 1024
	}

	if len(conf.DefaultBackend) == 0 {
		conf.DefaultBackend = "file"
	}

	var backendConfigs map[string]BackendConfig

	backendConfigs = map[string]BackendConfig{
		"file":      {Fs: lo.Ternary(conf.BackendConfig.Fs != nil, conf.BackendConfig.Fs, &fs.FSBackendConfig{})},
		"s3":        {S3: lo.Ternary(conf.BackendConfig.S3 != nil, conf.BackendConfig.S3, &s3.S3BackendConfig{})},
		"memory":    {Memory: lo.Ternary(conf.BackendConfig.Memory != nil, conf.BackendConfig.Memory, &memory.MemoryBackendConfig{})},
		"http":      {Http: lo.Ternary(conf.BackendConfig.Http != nil, conf.BackendConfig.Http, &http.HTTPBackendConfig{})},
		"https":     {Http: lo.Ternary(conf.BackendConfig.Http != nil, conf.BackendConfig.Http, &http.HTTPBackendConfig{})},
		"git+https": {Git: lo.Ternary(conf.BackendConfig.Git != nil, conf.BackendConfig.Git, &git.GitBackendConfig{})},
		"git+ssh":   {Git: lo.Ternary(conf.BackendConfig.Git != nil, conf.BackendConfig.Git, &git.GitBackendConfig{})},
	}

	if conf.Sftp != nil {
		backendConfigs["sftp"] = BackendConfig{Sftp: conf.BackendConfig.Sftp}
	}

	if conf.Redis != nil {
		backendConfigs["redis"] = BackendConfig{Redis: conf.BackendConfig.Redis}
	}

	if err := mergo.Merge(
		&backendConfigs,
		conf.Backends,
		mergo.WithOverride,
		mergo.WithSliceDeepCopy,
	); err != nil {
		return nil, err
	}

	var backends = make(map[string]backend.Backend)

	for scheme, backendConf := range backendConfigs {
		var (
			backend backend.Backend
			err     error
		)

		switch {
		case backendConf.Fs != nil:
			backend = fs.NewFSBackend(ctx, *backendConf.Fs)
		case backendConf.Memory != nil:
			backend = memory.NewMemoryBackend(ctx, *backendConf.Memory)
		case backendConf.Http != nil:
			backend = http.NewHTTPBackend(ctx, *backendConf.Http)
		case backendConf.S3 != nil:
			backend, err = s3.NewS3Backend(ctx, *backendConf.S3)
		case backendConf.Redis != nil:
			backend, err = redis.NewRedisBackend(ctx, *backendConf.Redis)
		case backendConf.Sftp != nil:
			backend = sftp.NewSFTPBackend(ctx, *backendConf.Sftp)
		case backendConf.Git != nil:
			backend, err = git.NewGitBackend(ctx, *backendConf.Git)
		default:
			return nil, fmt.Errorf("backend conf must be specified for scheme: %s", strings.ToLower(scheme))
		}

		if err != nil {
			return nil, err
		}

		backends[strings.ToLower(scheme)] = backend
	}

	be, found := backends[conf.DefaultBackend]

	if !found {
		return nil, fmt.Errorf("default backend %s does not exists", conf.DefaultBackend)
	}

	backends[""] = be

	return &ObjectStore{
		conf:     conf,
		backends: backends,
	}, nil
}

func MustNewObjectStore(ctx context.Context, conf Config) *ObjectStore {
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

	backend, found := os.backends[strings.ToLower(u.Scheme)]

	if !found {
		return nil, fmt.Errorf("no backend found for scheme %s", strings.ToLower(u.Scheme))
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
