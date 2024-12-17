package s3

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3ReaderAt struct {
	ctx context.Context
	s3  *s3.S3
	url *url.URL
}

func news3ReaderAt(ctx context.Context, s3 *s3.S3, url *url.URL) (*s3ReaderAt, error) {
	return &s3ReaderAt{
		ctx: ctx,
		s3:  s3,
		url: url,
	}, nil
}

func (s3r *s3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	var (
		rangeStart = off
		rangeEnd   = off + int64(len(p)) - 1
		_range     = fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)
	)

	input := &s3.GetObjectInput{}
	input = input.SetBucket(s3r.url.Host)
	input = input.SetKey(s3r.url.Path)
	input.Range = aws.String(_range)

	output, err := s3r.s3.GetObjectWithContext(s3r.ctx, input)

	if err != nil {
		return 0, processError(err)
	}

	defer output.Body.Close()

	n, err := io.ReadFull(output.Body, p)

	if err == io.ErrUnexpectedEOF && n == int(*output.ContentLength) {
		return n, nil
	}

	if err != nil {
		return n, err
	}

	return n, nil
}

func (s3r *s3ReaderAt) Close() error {
	return nil
}
