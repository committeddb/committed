package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/philborlin/committed/committed"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:8080", "comma separated http servers")
	port := flag.Int("port", 8080, "http server port")
	flag.Parse()

	committed.NewCluster(strings.Split(*cluster, ","), *port)
	fmt.Printf("Starting committed.\n")

	fmt.Scanln()
	fmt.Println("done")
}
