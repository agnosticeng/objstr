package s3

import (
	"github.com/agnosticeng/objstr/errors"
	"github.com/aws/aws-sdk-go/aws/awserr"
)

func processError(err error) error {
	if err == nil {
		return nil
	}

	switch err := err.(type) {
	case awserr.Error:
		switch err.Code() {
		case "NoSuchKey":
			return errors.ErrObjectNotFound
		default:
			return err
		}
	default:
		return err
	}
}
