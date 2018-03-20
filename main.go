package main

import (
	"fmt"

	"github.com/philborlin/committed/committed"
)

func main() {
	committed.NewCluster([]string{"http://127.0.0.1:8080"})
	fmt.Printf("Starting committed.\n")

	fmt.Scanln()
	fmt.Println("done")
}
