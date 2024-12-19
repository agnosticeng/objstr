package cli

import (
	"github.com/agnosticeng/cnf"
	"github.com/agnosticeng/objstr"
	"github.com/urfave/cli/v2"
)

func ObjStrBefore(opts ...cnf.OptionFunc) func(ctx *cli.Context) error {
	return func(ctx *cli.Context) error {
		var cfg objstr.Config

		if err := cnf.Load(&cfg, opts...); err != nil {
			return err
		}

		os, err := objstr.NewObjectStore(ctx.Context, cfg)

		if err != nil {
			return err
		}

		ctx.Context = objstr.NewContext(ctx.Context, os)
		return nil
	}
}

func ObjStrAfter(ctx *cli.Context) error {
	if os := objstr.FromContextOrDefault(ctx.Context); os != nil {
		return os.Close()
	}

	return nil
}
