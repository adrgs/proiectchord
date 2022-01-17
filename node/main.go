package main

import (
	"os"

	"github.com/adrgs/proiectchord/chord"
	"github.com/urfave/cli"
)

var conf config

func init() {
	conf = config{}
}

func main() {
	app := cli.NewApp()
	app.Name = "P2P-GRPC"
	app.Usage = "Simple p2p grpc Hello message service testing the limits of p2p"
	app.Flags = AppConfigFlags
	app.Version = "v0.0.1"
	app.Action = func(cli *cli.Context) error { return nil }
	app.Run(os.Args)

	node := &chord.Node{Name: conf.NodeName, Addr: conf.NodeAddr, SDAddress: conf.ServiceDiscoveryAddress}

	node.Start()
}
