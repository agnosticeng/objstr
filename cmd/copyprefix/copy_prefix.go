package copyprefix

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	"github.com/agnosticeng/objstr/utils"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:    "copyprefix",
		Aliases: []string{"cpp", "cpr"},
		Usage:   "<src> <dst>",
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

			srcPrefix, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			dstPrefix, err := url.Parse(ctx.Args().Get(1))

			if err != nil {
				return err
			}

			objects, err := store.ListPrefix(context.Background(), srcPrefix)

			if err != nil {
				return err
			}

			var mapper = iter.Mapper[*types.Object, error]{
				MaxGoroutines: maxConcurrentRequests,
			}

			var errs = mapper.Map(objects, func(o **types.Object) error {
				var src = (*o).URL

				dst, err := utils.GenerateDstURL(dstPrefix, srcPrefix, src)

				if err == nil && verbose {
					fmt.Println("from", src.String(), "to", dst.String())
				}

				return store.Copy(ctx.Context, src, dst)
			})

			return errors.Join(errs...)
		},
	}
}
