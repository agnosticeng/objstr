package objstr

import "context"

var _default *ObjectStore

func init() {
	_default = MustNewObjectStore(context.Background(), ObjectStoreConfig{})
}

func GetDefault() *ObjectStore {
	return _default
}
