package types

import (
	"net/url"
	"time"
)

type ObjectMetadata struct {
	Size             uint64
	ModificationDate time.Time
	ETag             string
}

type Object struct {
	URL      *url.URL
	Metadata *ObjectMetadata
}
