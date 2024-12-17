package sftp

import (
	"context"
	"net/url"
	"sync"

	"github.com/hashicorp/go-multierror"
)

type ClientCache struct {
	lock    sync.Mutex
	clients map[string]*Client
}

func NewClientCache() *ClientCache {
	return &ClientCache{
		clients: make(map[string]*Client),
	}
}

func (cache *ClientCache) Get(ctx context.Context, u *url.URL) (*Client, error) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	var key = u.User.Username() + "@" + u.Host

	if client, found := cache.clients[key]; found {
		return client, nil
	}

	client, err := NewClient(ctx, u.String())

	if err != nil {
		return nil, err
	}

	cache.clients[key] = client
	return client, nil
}

func (cache *ClientCache) Close() error {
	var res *multierror.Error

	for _, v := range cache.clients {
		if err := v.Close(); err != nil {
			res = multierror.Append(res, err)
		}
	}

	return res.ErrorOrNil()
}
