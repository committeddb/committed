package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/philborlin/committed/db"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:8080", "comma separated http servers")
	port := flag.Int("port", 8080, "http server port")
	// id := flag.Int("id", 1, "node ID")
	flag.Parse()

	db.NewCluster(strings.Split(*cluster, ","), *port)
	fmt.Printf("Starting committed.\n")

	for true {
	}

	fmt.Println("done")
}
