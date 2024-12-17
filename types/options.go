package types

type ListOptions struct {
	StartAfter string
}

type ListOption func(*ListOptions)

func WithStartAfter(s string) ListOption {
	return func(opts *ListOptions) {
		opts.StartAfter = s
	}
}

func NewListOptions(opts ...ListOption) *ListOptions {
	var res ListOptions

	for _, opt := range opts {
		opt(&res)
	}

	return &res
}
