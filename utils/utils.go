package utils

import (
	"bytes"
	"context"
	"io"
	"net/url"

	"github.com/agnosticeng/objstr"
)

func ReadObject(ctx context.Context, os *objstr.ObjectStore, u *url.URL) ([]byte, error) {
	r, err := os.Reader(ctx, u)

	if err != nil {
		return nil, err
	}

	defer r.Close()
	return io.ReadAll(r)
}

func CreateObject(ctx context.Context, os *objstr.ObjectStore, u *url.URL, data []byte) error {
	w, err := os.Writer(ctx, u)

	if err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewReader(data))
	return err
}
