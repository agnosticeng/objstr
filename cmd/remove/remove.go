package remove

import (
	"context"
	"net/url"

	"github.com/agnosticeng/objstr"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:    "remove",
		Aliases: []string{"rm"},
		Usage:   "<src>",
		Action: func(ctx *cli.Context) error {
			var store = objstr.FromContextOrDefault(ctx.Context)

			src, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			if err := store.Delete(context.Background(), src); err != nil {
				return err
			}

			return nil
		},
	}
}
