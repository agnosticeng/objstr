package http

import (
	"context"
	stderr "errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/agnosticeng/objstr/errors"
	"github.com/agnosticeng/objstr/types"
)

type HTTPBackendConfig struct{}

type HTTPBackend struct {
	client *http.Client
}

func NewHTTPBackend(ctx context.Context, conf HTTPBackendConfig) *HTTPBackend {
	return &HTTPBackend{
		client: &http.Client{},
	}
}

func (be *HTTPBackend) validateURL(u *url.URL) error {
	if len(u.Host) == 0 {
		return fmt.Errorf("host can't be empty")
	}

	return nil
}

func (be *HTTPBackend) ListPrefix(ctx context.Context, u *url.URL, optFunc ...types.ListOption) ([]*types.Object, error) {
	return nil, stderr.ErrUnsupported
}

func (be *HTTPBackend) ReadMetadata(ctx context.Context, u *url.URL) (*types.ObjectMetadata, error) {
	return nil, stderr.ErrUnsupported
}

func (be *HTTPBackend) Reader(ctx context.Context, u *url.URL) (types.Reader, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)

	if err != nil {
		return nil, err
	}

	resp, err := be.client.Do(req.WithContext(ctx))

	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 404 {
		return nil, errors.ErrObjectNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("invalid HTTP status code: %d", resp.StatusCode)
	}

	return resp.Body, nil
}

func (be *HTTPBackend) ReaderAt(ctx context.Context, u *url.URL) (types.ReaderAt, error) {
	return nil, stderr.ErrUnsupported
}

func (be *HTTPBackend) Writer(ctx context.Context, u *url.URL) (types.Writer, error) {
	return nil, stderr.ErrUnsupported

}

func (be *HTTPBackend) Delete(ctx context.Context, u *url.URL) error {
	return stderr.ErrUnsupported
}

func (be *HTTPBackend) Close() error {
	return nil
}
