package utils

import (
	"net/url"
	"path/filepath"
	"strings"
)

func GenerateDstURL(dstBase *url.URL, srcBase *url.URL, src *url.URL) (*url.URL, error) {
	u, err := url.Parse(dstBase.String())

	if err != nil {
		return nil, err
	}

	u.Path = filepath.Join(u.Path, strings.TrimPrefix(src.Path, srcBase.Path))
	return u, nil
}
