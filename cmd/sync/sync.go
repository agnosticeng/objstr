package sync

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
		Name:  "sync",
		Usage: "<left> <right>",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "max-concurrent-requests", Value: 100},
			&cli.BoolFlag{Name: "verbose"},
		},
		Action: func(ctx *cli.Context) error {
			var (
				os                    = objstr.FromContextOrDefault(ctx.Context)
				maxConcurrentRequests = ctx.Int("max-concurrent-requests")
				opts                  []types.ListOption
			)

			srcPrefix, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			dstPrefix, err := url.Parse(ctx.Args().Get(1))

			if err != nil {
				return err
			}

			srcObjs, err := os.ListPrefix(context.Background(), srcPrefix, opts...)

			if err != nil {
				return err
			}

			dstObjs, err := os.ListPrefix(context.Background(), dstPrefix, opts...)

			if err != nil {
				return err
			}

			var pairs = utils.Associate(
				srcPrefix,
				srcObjs,
				dstPrefix,
				dstObjs,
			)

			var mapper = iter.Mapper[utils.ObjectPair, error]{
				MaxGoroutines: maxConcurrentRequests,
			}

			var errs = mapper.Map(pairs, func(pair *utils.ObjectPair) error {
				switch {
				case pair.Left == nil:
					fmt.Println("DELETE", pair.Right.URL.String())
					return os.Delete(ctx.Context, pair.Right.URL)

				case pair.Right == nil:
					dstUrl, err := utils.GenerateDstURL(dstPrefix, srcPrefix, pair.Left.URL)

					if err != nil {
						return err
					}

					fmt.Println("COPY", pair.Left.URL.String(), dstUrl.String())
					return os.Copy(ctx.Context, pair.Left.URL, dstUrl)

				case pair.Left.Metadata.Size != pair.Right.Metadata.Size:
					return os.Copy(ctx.Context, pair.Left.URL, pair.Right.URL)

				default:
					return nil
				}
			})

			return errors.Join(errs...)
		},
	}
}
