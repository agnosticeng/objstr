package s3

import (
	"context"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/errgroup"
)

type s3WriterConfig struct {
	PartSize    int
	Concurrency int
	MaxParts    int
}
type s3Writer struct {
	conf  s3WriterConfig
	sess  *session.Session
	u     *url.URL
	r     *io.PipeReader
	w     *io.PipeWriter
	group *errgroup.Group
}

func newS3Writer(ctx context.Context, sess *session.Session, u *url.URL, conf s3WriterConfig) *s3Writer {
	r, w := io.Pipe()

	group, ctx := errgroup.WithContext(ctx)

	s3Writer := s3Writer{
		conf:  conf,
		sess:  sess,
		u:     u,
		r:     r,
		w:     w,
		group: group,
	}

	group.Go(s3Writer.runUploader)
	return &s3Writer
}

func (s3w *s3Writer) runUploader() error {
	defer s3w.r.Close()

	uploader := s3manager.NewUploader(s3w.sess, func(u *s3manager.Uploader) {
		u.PartSize = int64(s3w.conf.PartSize)
		u.MaxUploadParts = s3w.conf.MaxParts
		u.Concurrency = s3w.conf.Concurrency
	})

	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: &s3w.u.Host,
		Key:    &s3w.u.Path,
		Body:   s3w.r,
	})

	return err
}

func (s3w *s3Writer) Write(data []byte) (int, error) {
	return s3w.w.Write(data)
}

func (s3w *s3Writer) Close() error {
	var res *multierror.Error

	if err := s3w.w.Close(); err != nil {
		res = multierror.Append(res, err)
	}

	if err := s3w.group.Wait(); err != nil {
		res = multierror.Append(res, err)
	}

	return res.ErrorOrNil()
}
