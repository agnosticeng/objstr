package read

import (
	"errors"
	"io"
	"net/url"
	"os"

	"github.com/agnosticeng/objstr"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "read",
		Usage: "<src>",
		Action: func(ctx *cli.Context) error {
			var store = objstr.FromContextOrDefault(ctx.Context)

			src, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			r, err := store.Reader(ctx.Context, src)

			if err != nil {
				return err
			}

			for {
				var buf = make([]byte, 1024*1024)

				n, err := r.Read(buf)

				if n > 0 {
					if _, err := os.Stdout.Write(buf); err != nil {
						return err
					}
				}

				if errors.Is(err, io.EOF) {
					return nil
				}

				if err != nil {
					return err
				}
			}
		},
	}
}
