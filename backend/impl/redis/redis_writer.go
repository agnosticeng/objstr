package redis

import (
	"bytes"
	"context"

	"github.com/redis/rueidis"
)

type RedisWriter struct {
	client rueidis.Client
	key    string
	buf    bytes.Buffer
}

func NewRedisWriter(client rueidis.Client, key string) *RedisWriter {
	return &RedisWriter{client: client, key: key}
}

func (w *RedisWriter) Write(data []byte) (int, error) {
	return w.buf.Write(data)
}

func (w *RedisWriter) Close() error {
	return w.client.Do(context.Background(), w.client.B().Set().Key(w.key).Value(w.buf.String()).Build()).Error()
}
