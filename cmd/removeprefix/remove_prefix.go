package removeprefix

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:    "removeprefix",
		Aliases: []string{"rmp", "rmr"},
		Usage:   "<src>",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "max-concurrent-requests", Value: 100},
			&cli.BoolFlag{Name: "verbose"},
		},
		Action: func(ctx *cli.Context) error {
			var (
				store                 = objstr.FromContextOrDefault(ctx.Context)
				maxConcurrentRequests = ctx.Int("max-concurrent-requests")
				verbose               = ctx.Bool("verbose")
			)

			src, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			objects, err := store.ListPrefix(context.Background(), src)

			if err != nil {
				return err
			}

			var mapper = iter.Mapper[*types.Object, error]{
				MaxGoroutines: maxConcurrentRequests,
			}

			var errs = mapper.Map(objects, func(o **types.Object) error {
				var err = store.Delete(ctx.Context, (*o).URL)

				if err == nil && verbose {
					fmt.Println(((*o).URL).String())
				}

				return err
			})

			return errors.Join(errs...)
		},
	}
}
