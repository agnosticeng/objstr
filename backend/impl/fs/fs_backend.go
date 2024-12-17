package fs

import (
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/agnosticeng/objstr/errors"
	"github.com/agnosticeng/objstr/types"
)

type FSBackendConfig struct{}

type FSBackend struct{}

func NewFSBackend(ctx context.Context, conf FSBackendConfig) *FSBackend {
	return &FSBackend{}
}

func (be *FSBackend) validateURL(u *url.URL) error {
	if len(u.Path) == 0 && len(u.Host) == 0 {
		return fmt.Errorf("path and host can't both be empty")
	}

	return nil
}

func (be *FSBackend) ListPrefix(ctx context.Context, u *url.URL, optFuncs ...types.ListOption) ([]*types.Object, error) {
	var (
		opts       = types.NewListOptions(optFuncs...)
		res        []*types.Object
		pathPrefix = filepath.Join(u.Host, u.Path)
		dir        string
	)

	absPathPrefix, err := filepath.Abs(pathPrefix)

	if err != nil {
		return nil, err
	}

	absPathPrefixSegments := strings.Split(absPathPrefix, "/")

	for i := len(absPathPrefixSegments); i >= 0; i-- {
		dir = "/" + filepath.Join(absPathPrefixSegments[0:i]...)

		info, err := os.Stat(dir)

		if os.IsNotExist(err) {
			continue
		}

		if info.IsDir() {
			break
		}
	}

	err = filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		if !strings.HasPrefix(path, absPathPrefix) {
			return nil
		}

		if len(opts.StartAfter) > 0 {
			if strings.Compare(path, opts.StartAfter) <= 0 {
				return nil
			}
		}

		var obj types.Object

		obj.URL = &url.URL{
			Path: path,
		}

		obj.Metadata = &types.ObjectMetadata{
			Size:             uint64(info.Size()),
			ModificationDate: info.ModTime(),
		}

		res = append(res, &obj)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (be *FSBackend) ReadMetadata(ctx context.Context, u *url.URL) (*types.ObjectMetadata, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	stat, err := os.Stat(filepath.Join(u.Host, u.Path))

	if os.IsNotExist(err) {
		return nil, errors.ErrObjectNotFound
	}

	if err != nil {
		return nil, err
	}

	return &types.ObjectMetadata{
		Size:             uint64(stat.Size()),
		ModificationDate: stat.ModTime(),
	}, nil
}

func (be *FSBackend) Reader(ctx context.Context, u *url.URL) (types.Reader, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	f, err := os.Open(filepath.Join(u.Host, u.Path))

	if os.IsNotExist(err) {
		return nil, errors.ErrObjectNotFound
	}

	return f, nil
}

func (be *FSBackend) ReaderAt(ctx context.Context, u *url.URL) (types.ReaderAt, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	f, err := os.Open(filepath.Join(u.Host, u.Path))

	if os.IsNotExist(err) {
		return nil, errors.ErrObjectNotFound
	}

	return f, nil
}

func (be *FSBackend) Writer(ctx context.Context, u *url.URL) (types.Writer, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	path := filepath.Join(u.Host, u.Path)

	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return nil, err
	}

	return os.Create(path)
}

func (be *FSBackend) Delete(ctx context.Context, u *url.URL) error {
	if err := be.validateURL(u); err != nil {
		return err
	}

	return os.Remove(filepath.Join(u.Host, u.Path))
}

func (be *FSBackend) Move(ctx context.Context, src *url.URL, dst *url.URL) error {
	if err := be.validateURL(src); err != nil {
		return err
	}

	if err := be.validateURL(dst); err != nil {
		return err
	}

	srcPath := filepath.Join(src.Host, src.Path)
	dstPath := filepath.Join(dst.Host, dst.Path)

	if err := os.MkdirAll(filepath.Dir(dstPath), os.ModePerm); err != nil {
		return err
	}

	return os.Rename(srcPath, dstPath)
}

func (be *FSBackend) Close() error {
	return nil
}
