/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/philborlin/committed/cmd"
)

func main() {
	// Initialize the global zap logger before cmd.Execute. Without
	// this, every zap.L() call in the codebase resolves to the Nop
	// logger and operators have zero visibility into ingest worker
	// supervisor errors, raft handoffs, propose failures, etc.
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	zap.ReplaceGlobals(logger)
	defer func() { _ = logger.Sync() }()

	cmd.Execute()
}
