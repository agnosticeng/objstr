package s3

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/url"

	"github.com/agnosticeng/concu/mapstream"
	"github.com/agnosticeng/errgroup"
	"github.com/hashicorp/go-multierror"

	"github.com/aws/aws-sdk-go/service/s3"
)

type s3ReaderConfig struct {
	PartSize    int
	Concurrency int
}

type s3Reader struct {
	conf     s3ReaderConfig
	u        *url.URL
	r        *io.PipeReader
	group    *errgroup.Group
	groupCtx context.Context
}

func newS3Reader(ctx context.Context, svc *s3.S3, u *url.URL, conf s3ReaderConfig) (*s3Reader, error) {
	if conf.Concurrency <= 0 {
		conf.Concurrency = 1
	}

	if conf.PartSize <= 0 {
		conf.PartSize = 32 * 1024 * 1024
	}

	var input = &s3.HeadObjectInput{}
	input = input.SetBucket(u.Host)
	input = input.SetKey(u.Path)

	output, err := svc.HeadObjectWithContext(ctx, input)

	if err != nil {
		return nil, processError(err)
	}

	r, w := io.Pipe()

	group, groupCtx := errgroup.WithContext(ctx)

	s3Reader := s3Reader{
		conf:     conf,
		u:        u,
		r:        r,
		group:    group,
		groupCtx: groupCtx,
	}

	var (
		size    = *output.ContentLength
		parts   = math.Ceil(float64(size) / float64(conf.PartSize))
		inChan  = make(chan *s3.GetObjectInput, int(parts))
		outChan = make(chan []byte, 10)
	)

	for i := 0; i < int(parts); i++ {
		var (
			_range = fmt.Sprintf(
				"bytes=%d-%d",
				i*conf.PartSize,
				min((i*conf.PartSize)+conf.PartSize-1, int(size)-1),
			)
			input = &s3.GetObjectInput{}
		)

		input = input.SetBucket(u.Host)
		input = input.SetKey(u.Path)
		input = input.SetRange(_range)

		inChan <- input
	}

	close(inChan)

	group.Go(func() error {
		defer close(outChan)

		return mapstream.MapStream(
			groupCtx,
			inChan,
			outChan,
			func(ctx context.Context, input *s3.GetObjectInput) ([]byte, error) {
				return s3Reader.download(ctx, svc, input)
			},
			mapstream.MapStreamConfig{
				PoolSize: conf.Concurrency,
			},
		)
	})

	group.Go(func() error {
		return s3Reader.process(groupCtx, outChan, w)
	})

	return &s3Reader, nil
}

func (s3r *s3Reader) download(
	ctx context.Context,
	svc *s3.S3,
	input *s3.GetObjectInput,
) ([]byte, error) {
	output, err := svc.GetObjectWithContext(ctx, input)

	if err != nil {
		return nil, err
	}

	defer output.Body.Close()

	var p = make([]byte, s3r.conf.PartSize)

	n, err := io.ReadFull(output.Body, p)

	if err == io.ErrUnexpectedEOF && n == int(*output.ContentLength) {
		p = p[0:n]
		return p, nil
	}

	if err != nil {
		return p, err
	}

	return p, nil
}

func (s3r *s3Reader) process(ctx context.Context, inChan chan []byte, w io.WriteCloser) error {
	defer w.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case b, open := <-inChan:
			if !open {
				return nil
			}

			_, err := w.Write(b)

			if err != nil {
				return err
			}
		}
	}
}

func (s3r *s3Reader) Read(p []byte) (int, error) {
	select {
	case <-s3r.groupCtx.Done():
		return 0, s3r.groupCtx.Err()
	default:
	}

	n, err := s3r.r.Read(p)

	if err != nil {
		return n, err
	}

	return n, nil
}

func (s3r *s3Reader) Close() error {
	var res *multierror.Error

	res = multierror.Append(s3r.r.Close())
	res = multierror.Append(s3r.group.Wait())

	return res.ErrorOrNil()
}
