package main

import (
	"log"
	"os"

	"github.com/agnosticeng/cliutils"
	"github.com/agnosticeng/cnf"
	"github.com/agnosticeng/cnf/providers/env"
	objstrcli "github.com/agnosticeng/objstr/cli"
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

func main() {
	app := cli.App{
		Name: "objstr",
		Before: cliutils.CombineBeforeFuncs(
			slogcli.SlogBefore,
			objstrcli.ObjStrBefore(cnf.WithProvider(env.NewEnvProvider("OBJSTR"))),
		),
		After: cliutils.CombineAfterFuncs(
			objstrcli.ObjStrAfter,
			slogcli.SlogAfter,
		),
		Flags: slogcli.SlogFlags(),
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
