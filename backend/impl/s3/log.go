package s3

import "github.com/aws/aws-sdk-go/aws"

func logLevelTypeFromString(s string) aws.LogLevelType {
	switch s {
	case "DEBUG", "debug":
		return aws.LogDebug
	case "DEBUG_WITH_SIGNING", "debug_with_signing":
		return aws.LogDebugWithSigning
	case "DEBUG_WITH_HTTP_BODY", "debug_with_http_body":
		return aws.LogDebugWithHTTPBody
	case "DEBUG_WITH_EVENT_STREAM_BODY", "debug_with_event_stream_body":
		return aws.LogDebugWithEventStreamBody
	default:
		return aws.LogOff
	}
}
