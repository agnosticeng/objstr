package s3

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/agnosticeng/objstr/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	slogctx "github.com/veqryn/slog-context"
)

type S3BackendConfig struct {
	AccessKeyId         string
	SecretAccessKey     string
	SessionToken        string
	Endpoint            string
	Region              string
	DisableSsl          bool
	ForcePathStyle      bool
	UploadPartSize      int
	UploadConcurrency   int
	UploadMaxParts      int
	DownloadPartSize    int
	DownloadConcurrency int
	EnableAwsSdkLogging bool
	AwsSdkLogLevel      string
}

type S3Backend struct {
	conf       S3BackendConfig
	logger     *slog.Logger
	awsConf    *aws.Config
	awsSession *session.Session
	s3Svc      *s3.S3
}

func NewS3Backend(ctx context.Context, conf S3BackendConfig) (*S3Backend, error) {
	var (
		logger  = slogctx.FromCtx(ctx)
		awsConf = aws.NewConfig()
	)

	if len(conf.AccessKeyId) > 0 {
		awsConf = awsConf.WithCredentials(credentials.NewStaticCredentials(conf.AccessKeyId, conf.SecretAccessKey, conf.SessionToken))
	}

	if len(conf.Endpoint) > 0 {
		awsConf = awsConf.WithEndpoint(conf.Endpoint)
	}

	if len(conf.Region) > 0 {
		awsConf = awsConf.WithRegion(conf.Region)
	} else {
		awsConf = awsConf.WithRegion("auto")
	}

	awsConf = awsConf.WithDisableSSL(conf.DisableSsl)
	awsConf = awsConf.WithS3ForcePathStyle(conf.ForcePathStyle)

	if conf.EnableAwsSdkLogging {
		awsConf = awsConf.WithLogger(aws.LoggerFunc(func(args ...interface{}) {
			logger.Debug("AWS SDk LOG", "content", fmt.Sprint(args...))
		}))

		awsConf = awsConf.WithLogLevel(logLevelTypeFromString(conf.AwsSdkLogLevel))
	}

	awsSession, err := session.NewSession(awsConf)

	if err != nil {
		return nil, err
	}

	return &S3Backend{
		conf:       conf,
		logger:     slogctx.FromCtx(ctx),
		awsConf:    awsConf,
		awsSession: awsSession,
		s3Svc:      s3.New(awsSession),
	}, nil
}

func (be *S3Backend) validateURL(u *url.URL) error {
	if len(u.Host) == 0 {
		return fmt.Errorf("bucket must be specified")
	}

	return nil
}

func (be *S3Backend) ListPrefix(ctx context.Context, u *url.URL, optFunc ...types.ListOption) ([]*types.Object, error) {
	var (
		opts      = types.NewListOptions(optFunc...)
		res       []*types.Object
		remaining = true
		contToken *string
	)

	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	for remaining {
		input := &s3.ListObjectsV2Input{}
		input.SetBucket(u.Host)
		input.SetPrefix(strings.TrimPrefix(u.Path, "/"))

		if len(opts.StartAfter) > 0 {
			startAfterUrl, err := url.Parse(opts.StartAfter)

			if err != nil {
				return nil, err
			}

			var startAfterKey = strings.TrimPrefix(startAfterUrl.Path, "/")
			input.StartAfter = &startAfterKey
		}

		if contToken != nil && len(*contToken) > 0 {
			input.ContinuationToken = contToken
		}

		output, err := be.s3Svc.ListObjectsV2WithContext(ctx, input)

		if err != nil {
			return nil, err
		}

		remaining = *output.IsTruncated
		contToken = output.NextContinuationToken

		for _, object := range output.Contents {
			var obj = types.Object{
				URL: &url.URL{
					Host: *output.Name,
					Path: "/" + *object.Key,
				},
				Metadata: &types.ObjectMetadata{},
			}

			if object.Size != nil {
				obj.Metadata.Size = uint64(*object.Size)
			}

			if object.LastModified != nil {
				obj.Metadata.ModificationDate = *object.LastModified
			}

			if object.ETag != nil {
				obj.Metadata.ETag = *object.ETag
			}

			res = append(res, &obj)
		}
	}

	return res, nil
}

func (be *S3Backend) ReadMetadata(ctx context.Context, u *url.URL) (*types.ObjectMetadata, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	var (
		input = &s3.HeadObjectInput{}
		md    types.ObjectMetadata
	)

	input = input.SetBucket(u.Host)
	input = input.SetKey(u.Path)

	info, err := be.s3Svc.HeadObjectWithContext(ctx, input)

	if err != nil {
		return nil, processError(err)
	}

	if info.ContentLength != nil {
		md.Size = uint64(*info.ContentLength)
	}

	if info.LastModified != nil {
		md.ModificationDate = *info.LastModified
	}

	if info.ETag != nil {
		md.ETag = *info.ETag
	}

	return &md, nil
}

func (be *S3Backend) Reader(ctx context.Context, u *url.URL) (types.Reader, error) {
	if be.conf.DownloadConcurrency <= 1 {
		return be.readSerial(ctx, u)
	}

	return be.readParallel(ctx, u)
}

func (be *S3Backend) readSerial(ctx context.Context, u *url.URL) (types.Reader, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	input := &s3.GetObjectInput{}
	input = input.SetBucket(u.Host)
	input = input.SetKey(u.Path)

	output, err := be.s3Svc.GetObjectWithContext(ctx, input)

	if err != nil {
		return nil, processError(err)
	}

	return output.Body, nil
}

func (be *S3Backend) readParallel(ctx context.Context, u *url.URL) (types.Reader, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	s3rConf := s3ReaderConfig{}
	s3rConf.PartSize = be.conf.DownloadPartSize
	s3rConf.Concurrency = be.conf.DownloadConcurrency

	r, err := newS3Reader(
		ctx,
		be.s3Svc,
		u,
		s3rConf,
	)

	if err != nil {
		return nil, processError(err)
	}

	return r, nil
}

func (be *S3Backend) ReaderAt(ctx context.Context, u *url.URL) (types.ReaderAt, error) {
	return news3ReaderAt(ctx, be.s3Svc, u)
}

func (be *S3Backend) Writer(ctx context.Context, u *url.URL) (types.Writer, error) {
	if err := be.validateURL(u); err != nil {
		return nil, err
	}

	s3wConf := s3WriterConfig{}
	s3wConf.Concurrency = be.conf.UploadConcurrency
	s3wConf.MaxParts = be.conf.UploadMaxParts
	s3wConf.PartSize = be.conf.UploadPartSize

	return newS3Writer(
		ctx,
		be.awsSession,
		u,
		s3wConf,
	), nil
}

func (be *S3Backend) Delete(ctx context.Context, u *url.URL) error {
	if err := be.validateURL(u); err != nil {
		return err
	}

	input := &s3.DeleteObjectInput{}
	input = input.SetBucket(u.Host)
	input = input.SetKey(u.Path)

	_, err := be.s3Svc.DeleteObjectWithContext(ctx, input)

	if err != nil {
		return err
	}

	return nil
}

func (be *S3Backend) Close() error {
	return nil
}
