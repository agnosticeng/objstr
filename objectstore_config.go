package objstr

import (
	"github.com/agnosticeng/objstr/backend/impl/fs"
	"github.com/agnosticeng/objstr/backend/impl/http"
	"github.com/agnosticeng/objstr/backend/impl/memory"
	"github.com/agnosticeng/objstr/backend/impl/redis"
	"github.com/agnosticeng/objstr/backend/impl/s3"
	"github.com/agnosticeng/objstr/backend/impl/sftp"
)

type ObjectStoreBackendConfig struct {
	Scheme string
	Mem    *memory.MemoryBackendConfig
	Fs     *fs.FSBackendConfig
	Http   *http.HTTPBackendConfig
	S3     *s3.S3BackendConfig
	Sftp   *sftp.SFTPBackendConfig
	Redis  *redis.RedisBackendConfig
}

type ObjectStoreConfig struct {
	CopyBufferSize         int
	DisableDefaultBackends bool
	Backends               []ObjectStoreBackendConfig
}

func (c *ObjectStoreConfig) WithBackend(backendConf ObjectStoreBackendConfig) *ObjectStoreConfig {
	c.Backends = append(c.Backends, backendConf)
	return c
}

func (c *ObjectStoreConfig) WithCopyBufferSize(size int) *ObjectStoreConfig {
	c.CopyBufferSize = size
	return c
}

func (c *ObjectStoreConfig) WithDisableDefaultBackends(b bool) *ObjectStoreConfig {
	c.DisableDefaultBackends = b
	return c
}
