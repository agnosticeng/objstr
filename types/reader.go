package types

import "io"

type Reader interface {
	io.ReadCloser
}

type ReaderAt interface {
	io.Closer
	io.ReaderAt
}
