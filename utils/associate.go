package utils

import (
	"net/url"
	"strings"

	"github.com/agnosticeng/objstr/types"
	"github.com/samber/lo"
)

type ObjectPair struct {
	Path  string
	Left  *types.Object
	Right *types.Object
}

func Associate(
	leftPrefix *url.URL,
	leftObjects []*types.Object,
	rightPrefix *url.URL,
	rightObjects []*types.Object,
) []ObjectPair {
	var (
		res            []ObjectPair
		leftObjectsIdx = lo.Associate(leftObjects, func(o *types.Object) (string, *types.Object) {
			return keyedObject(leftPrefix, o)
		})
		rightObjectsIdx = lo.Associate(rightObjects, func(o *types.Object) (string, *types.Object) {
			return keyedObject(rightPrefix, o)
		})
	)

	for _, leftObject := range leftObjects {
		res = append(res, ObjectPair{
			Path:  leftObject.URL.Path,
			Left:  leftObject,
			Right: rightObjectsIdx[leftObject.URL.Path],
		})
	}

	for _, rightObject := range rightObjects {
		var leftObject = leftObjectsIdx[rightObject.URL.Path]

		if leftObject == nil {
			res = append(res, ObjectPair{
				Path:  rightObject.URL.Path,
				Left:  nil,
				Right: rightObjectsIdx[rightObject.URL.Path],
			})
		}
	}

	return res

}

func keyedObject(prefix *url.URL, obj *types.Object) (string, *types.Object) {
	return strings.TrimPrefix(obj.URL.Path, prefix.Path), obj
}
