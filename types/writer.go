package types

import "io"

type Writer interface {
	io.WriteCloser
}
