package main

import (
	"flag"
	"strings"

	"github.com/philborlin/committed/syncable"

	"github.com/philborlin/committed/db"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	apiPort := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	nodes := strings.Split(*cluster, ",")

	db.NewCluster(nodes, *id, *apiPort, *join)

	syncable.Parse("", nil)
}
