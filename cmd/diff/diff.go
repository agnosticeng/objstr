package diff

import (
	"context"
	"fmt"
	"net/url"

	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	"github.com/agnosticeng/objstr/utils"
	"github.com/dustin/go-humanize"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "diff",
		Usage: "<left> <right>",
		Action: func(ctx *cli.Context) error {
			var (
				os   = objstr.FromContextOrDefault(ctx.Context)
				opts []types.ListOption
			)

			leftU, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			rightU, err := url.Parse(ctx.Args().Get(1))

			if err != nil {
				return err
			}

			leftObjs, err := os.ListPrefix(context.Background(), leftU, opts...)

			if err != nil {
				return err
			}

			rightObjs, err := os.ListPrefix(context.Background(), rightU, opts...)

			if err != nil {
				return err
			}

			var pairs = utils.Associate(
				leftU,
				leftObjs,
				rightU,
				rightObjs,
			)

			for _, pair := range pairs {
				switch {
				case pair.Right == nil:
					fmt.Println("RIGHT MISSING", pair.Path)
				case pair.Left == nil:
					fmt.Println("LEFT MISSING", pair.Path)
				case pair.Left.Metadata.Size != pair.Right.Metadata.Size:
					fmt.Println(
						"SIZE DIFFERS", pair.Path,
						"LEFT", humanize.Bytes(pair.Left.Metadata.Size),
						"RIGHT", humanize.Bytes(pair.Right.Metadata.Size),
					)
				}
			}

			var (
				leftFiles  uint64
				rightFiles uint64
				leftSize   uint64
				rightSize  uint64
			)

			for _, pair := range pairs {
				if pair.Left != nil {
					leftFiles++
					leftSize += pair.Left.Metadata.Size
				}

				if pair.Right != nil {
					rightFiles++
					rightSize += pair.Right.Metadata.Size
				}
			}

			fmt.Println()
			fmt.Println("files", leftFiles, rightFiles)
			fmt.Println("sizes", humanize.Bytes(leftSize), humanize.Bytes(rightSize))

			return nil
		},
	}
}
