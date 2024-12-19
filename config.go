package objstr

import (
	"github.com/agnosticeng/objstr/backend/impl/fs"
	"github.com/agnosticeng/objstr/backend/impl/http"
	"github.com/agnosticeng/objstr/backend/impl/memory"
	"github.com/agnosticeng/objstr/backend/impl/redis"
	"github.com/agnosticeng/objstr/backend/impl/s3"
	"github.com/agnosticeng/objstr/backend/impl/sftp"
)

type BackendConfig struct {
	Memory *memory.MemoryBackendConfig
	Fs     *fs.FSBackendConfig
	Http   *http.HTTPBackendConfig
	S3     *s3.S3BackendConfig
	Sftp   *sftp.SFTPBackendConfig
	Redis  *redis.RedisBackendConfig
}

type Config struct {
	CopyBufferSize int
	DefaultBackend string
	BackendConfig
	Backends map[string]BackendConfig
}

func (c *Config) WithBackend(scheme string, backendConf BackendConfig) *Config {
	c.Backends[scheme] = backendConf
	return c
}

func (c *Config) WithCopyBufferSize(size int) *Config {
	c.CopyBufferSize = size
	return c
}
