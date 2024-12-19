package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/agnosticeng/objstr"
	"gopkg.in/yaml.v2"
)

type CnfProvider struct {
	os   *objstr.ObjectStore
	path string
}

func NewCnfProvider(os *objstr.ObjectStore, path string) *CnfProvider {
	return &CnfProvider{os: os, path: path}
}

func (p *CnfProvider) ReadMap() (map[string]interface{}, error) {
	u, err := url.Parse(p.path)

	if err != nil {
		return nil, err
	}

	content, err := ReadObject(context.Background(), p.os, u)

	if err != nil {
		return nil, err
	}

	var unmarshaler func([]byte, interface{}) error

	switch filepath.Ext(u.Path) {
	case ".json":
		unmarshaler = json.Unmarshal
	case ".yaml", ".yml":
		unmarshaler = yaml.Unmarshal
	default:
		return nil, fmt.Errorf("unhandled file extension: %s", filepath.Ext(p.path))
	}

	var m map[string]interface{}

	if err := unmarshaler(content, &m); err != nil {
		return nil, err
	}

	return m, nil
}
