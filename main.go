/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/committeddb/committed/cmd"
)

func main() {
	// Initialize the global zap logger before cmd.Execute. Without
	// this, every zap.L() call in the codebase resolves to the Nop
	// logger and operators have zero visibility into ingest worker
	// supervisor errors, raft handoffs, propose failures, etc.
	cfg := zap.NewProductionConfig()
	// COMMITTED_LOG_LEVEL overrides the default Info level (debug|info|warn|
	// error|dpanic|panic|fatal). Set it to "debug" to surface the finer-grained
	// diagnostics (e.g. raft-log compaction cadence) that are otherwise silent.
	// A bad value fails fast rather than silently staying at Info — an operator
	// who asked for debug during an incident must not be left wondering why the
	// log looks unchanged.
	if raw := os.Getenv("COMMITTED_LOG_LEVEL"); raw != "" {
		lvl, err := zap.ParseAtomicLevel(raw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid COMMITTED_LOG_LEVEL %q (want one of debug, info, warn, error): %v\n", raw, err)
			os.Exit(1)
		}
		cfg.Level = lvl
	}
	logger, err := cfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	zap.ReplaceGlobals(logger)
	defer func() { _ = logger.Sync() }()

	cmd.Execute()
}
