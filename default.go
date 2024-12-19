package objstr

import "context"

var _default *ObjectStore

func init() {
	_default = MustNewObjectStore(context.Background(), Config{})
}

func GetDefault() *ObjectStore {
	return _default
}
