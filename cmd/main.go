package main

import (
	"log"
	"os"

	"github.com/agnosticeng/cnf"
	"github.com/agnosticeng/cnf/providers/env"
	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/cmd/copy"
	"github.com/agnosticeng/objstr/cmd/copyprefix"
	"github.com/agnosticeng/objstr/cmd/diff"
	"github.com/agnosticeng/objstr/cmd/list"
	"github.com/agnosticeng/objstr/cmd/read"
	"github.com/agnosticeng/objstr/cmd/remove"
	"github.com/agnosticeng/objstr/cmd/removeprefix"
	"github.com/agnosticeng/objstr/cmd/sync"
	"github.com/agnosticeng/slogcli"
	"github.com/urfave/cli/v2"
)

func setup(ctx *cli.Context) error {
	if err := slogcli.SlogBefore(ctx); err != nil {
		return err
	}

	var cfg objstr.ObjectStoreConfig

	if err := cnf.Load(
		&cfg,
		cnf.WithProvider(env.NewEnvProvider("OBJSTR")),
	); err != nil {
		return err
	}

	store, err := objstr.NewObjectStore(ctx.Context, cfg)

	if err != nil {
		return err
	}

	ctx.Context = objstr.NewContext(ctx.Context, store)
	return nil
}

func main() {
	app := cli.App{
		Name:   "objstr",
		Before: setup,
		After:  slogcli.SlogAfter,
		Flags:  slogcli.SlogFlags(),
		Commands: []*cli.Command{
			list.Command(),
			copy.Command(),
			remove.Command(),
			read.Command(),
			removeprefix.Command(),
			copyprefix.Command(),
			diff.Command(),
			sync.Command(),
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err.Error())
	}
}
