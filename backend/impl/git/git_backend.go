package git

import (
	"context"
	stderr "errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/agnosticeng/objstr/errors"
	"github.com/agnosticeng/objstr/types"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/google/uuid"
)

type GitBackendConfig struct {
	TmpDir   string
	Matchers []string
}

type GitBackend struct {
	conf             GitBackendConfig
	cachedClonesLock sync.Mutex
	cachedClones     map[string]*object.Commit
	matchers         []*regexp.Regexp
}

func NewGitBackend(ctx context.Context, conf GitBackendConfig) (*GitBackend, error) {
	if len(conf.TmpDir) == 0 {
		conf.TmpDir = os.TempDir()
	}

	if len(conf.Matchers) == 0 {
		conf.Matchers = defaultMatchers
	}

	var matchers []*regexp.Regexp

	for _, v := range conf.Matchers {
		matcher, err := regexp.Compile(v)

		if err != nil {
			return nil, err
		}

		matchers = append(matchers, matcher)
	}

	return &GitBackend{
		conf:         conf,
		matchers:     matchers,
		cachedClones: make(map[string]*object.Commit),
	}, nil
}

func (be *GitBackend) getOrCloneCommit(ctx context.Context, fl *fileLocation) (*object.Commit, error) {
	be.cachedClonesLock.Lock()
	defer be.cachedClonesLock.Unlock()

	if commit, found := be.cachedClones[fl.Repository.String()+fl.Ref]; found {
		return commit, nil
	}

	commit, err := be.cloneCommit(ctx, fl)

	if err != nil {
		return nil, err
	}

	be.cachedClones[fl.Repository.String()+fl.Ref] = commit
	return commit, nil
}

func (be *GitBackend) cloneCommit(ctx context.Context, fl *fileLocation) (*object.Commit, error) {
	var (
		clonePath = filepath.Join(be.conf.TmpDir, uuid.Must(uuid.NewV7()).String())
		opts      git.CloneOptions
	)

	opts.URL = fl.Repository.String()
	opts.SingleBranch = true

	if len(fl.Ref) > 0 {
		opts.ReferenceName = plumbing.ReferenceName(fl.Ref)
	}

	fmt.Println(opts.URL)
	fmt.Println(opts.ReferenceName)

	r, err := git.PlainCloneContext(ctx, clonePath, true, &opts)

	if err != nil {
		return nil, err
	}

	ref, err := r.Head()

	if err != nil {
		return nil, err
	}

	commit, err := r.CommitObject(ref.Hash())

	if err != nil {
		return nil, err
	}

	return commit, nil
}

func (be *GitBackend) ListPrefix(ctx context.Context, u *url.URL, optFunc ...types.ListOption) ([]*types.Object, error) {
	fl, err := parseFileLocation(u.String(), be.matchers)

	if err != nil {
		return nil, err
	}

	commit, err := be.getOrCloneCommit(ctx, fl)

	if err != nil {
		return nil, err
	}

	it, err := commit.Files()

	if err != nil {
		return nil, err
	}

	var res []*types.Object

	for {
		f, err := it.Next()

		if stderr.Is(err, io.EOF) {
			break
		}

		if !strings.HasPrefix(f.Name, strings.TrimPrefix(fl.Path, "/")) {
			continue
		}

		var fileName = strings.TrimPrefix(f.Name, strings.TrimPrefix(fl.Path, "/"))

		var obj = types.Object{
			Metadata: &types.ObjectMetadata{
				Size: uint64(f.Size),
				ETag: f.Hash.String(),
			},
		}

		var objUrl, _ = url.Parse(u.String())
		objUrl.Path = filepath.Join(u.Path, fileName)

		if len(fl.Ref) > 0 {
			objUrl.Query().Set("ref", fl.Ref)
		}

		obj.URL = objUrl
		res = append(res, &obj)
	}

	return res, nil
}

func (be *GitBackend) ReadMetadata(ctx context.Context, u *url.URL) (*types.ObjectMetadata, error) {
	fl, err := parseFileLocation(u.String(), be.matchers)

	if err != nil {
		return nil, err
	}

	commit, err := be.getOrCloneCommit(ctx, fl)

	if err != nil {
		return nil, err
	}

	f, err := commit.File(strings.TrimPrefix(fl.Path, "/"))

	if stderr.Is(err, object.ErrFileNotFound) {
		return nil, errors.ErrObjectNotFound
	}

	return &types.ObjectMetadata{
		Size: uint64(f.Size),
		ETag: f.Hash.String(),
	}, nil
}

func (be *GitBackend) Reader(ctx context.Context, u *url.URL) (types.Reader, error) {
	fl, err := parseFileLocation(u.String(), be.matchers)

	if err != nil {
		return nil, err
	}

	commit, err := be.getOrCloneCommit(ctx, fl)

	if err != nil {
		return nil, err
	}

	f, err := commit.File(strings.TrimPrefix(fl.Path, "/"))

	if stderr.Is(err, object.ErrFileNotFound) {
		return nil, errors.ErrObjectNotFound
	}

	return f.Reader()
}

func (be *GitBackend) ReaderAt(ctx context.Context, u *url.URL) (types.ReaderAt, error) {
	return nil, stderr.ErrUnsupported
}

func (be *GitBackend) Writer(ctx context.Context, u *url.URL) (types.Writer, error) {
	return nil, stderr.ErrUnsupported
}

func (be *GitBackend) Delete(ctx context.Context, u *url.URL) error {
	return stderr.ErrUnsupported
}

func (be *GitBackend) Close() error {
	return nil
}
