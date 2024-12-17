package list

import (
	"context"
	"fmt"
	"net/url"

	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	"github.com/dustin/go-humanize"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:    "list",
		Aliases: []string{"ls"},
		Usage:   "<prefix>",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "start-after"},
		},
		Action: func(ctx *cli.Context) error {
			var (
				os         = objstr.FromContextOrDefault(ctx.Context)
				startAfter = ctx.String("start-after")
				opts       []types.ListOption
			)

			u, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			if len(startAfter) > 0 {
				opts = append(opts, types.WithStartAfter(startAfter))
			}

			objects, err := os.ListPrefix(context.Background(), u, opts...)

			if err != nil {
				return err
			}

			var totalSize uint64

			for _, object := range objects {
				fmt.Println(object.URL.String(), humanize.Bytes(object.Metadata.Size), object.Metadata.ModificationDate)
				totalSize += object.Metadata.Size
			}

			fmt.Println()
			fmt.Println("total", humanize.Bytes(totalSize))
			return nil
		},
	}
}
