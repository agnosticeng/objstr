package copy

import (
	"context"
	"net/url"
	"path"
	"strings"

	"github.com/agnosticeng/objstr"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:    "copy",
		Aliases: []string{"cp"},
		Usage:   "<src> <dst>",
		Action: func(ctx *cli.Context) error {
			var store = objstr.FromContextOrDefault(ctx.Context)

			src, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			dst, err := url.Parse(ctx.Args().Get(1))

			if err != nil {
				return err
			}

			if strings.HasSuffix(dst.Path, "/") {
				dst.Path = path.Join(dst.Path, path.Base(src.Path))
			}

			if err := store.Copy(context.Background(), src, dst); err != nil {
				return err
			}

			return nil
		},
	}
}
