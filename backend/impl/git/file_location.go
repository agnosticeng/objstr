package git

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// git+https://github.com/agnosticeng/ETLs/stakedao/project01/pipeline.yaml&ref=main
// git+ssh://github.com/agnosticeng/ETLs/stakedao/project01/pipeline.yaml&ref=main

var (
	defaultMatchers = []string{
		`^([^/]+/[^/]+/[^/]+)([/]?.*)`,
	}
)

type fileLocation struct {
	Repository *url.URL
	Ref        string
	Path       string
}

func parseFileLocation(s string, matchers []*regexp.Regexp) (*fileLocation, error) {
	u, err := url.Parse(s)

	if err != nil {
		return nil, err
	}

	var res = fileLocation{
		Ref: u.Query().Get("ref"),
	}

	switch u.Scheme {
	case "git+https":
		u.Scheme = "https"
	case "git+ssh":
		u.Scheme = "ssh"
	default:
		return nil, fmt.Errorf("unhandled git schem: %s", u.Scheme)
	}

	var (
		match     []string
		candidate = u.Host + u.Path
	)

	for _, matcher := range matchers {
		match = matcher.FindStringSubmatch(candidate)

		if len(match) == 3 {
			break
		}
	}

	if len(match) != 3 {
		return nil, fmt.Errorf("invalid git url: %s", s)
	}

	var q = u.Query()
	q.Del("ref")
	u.RawQuery = q.Encode()
	u.Path = strings.TrimSuffix(u.Path, match[2])
	res.Repository = u
	res.Path = match[2]

	return &res, nil
}

func (fl *fileLocation) ToURL() *url.URL {
	var res = fl.Repository.JoinPath(fl.Path)
	res.Scheme = "git+" + res.Scheme

	if len(fl.Ref) > 0 {
		res.Query().Set("ref", fl.Ref)
	}

	return res
}
