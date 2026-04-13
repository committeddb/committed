//go:build docker || integration

// This file ensures `go mod tidy` retains the testcontainers-go dependencies
// used by mysql_test.go. Without an import that go mod tidy can see (test
// files alone are not enough when behind a build tag), the deps would be
// stripped on the next `go mod tidy` run.
package mysql

import (
	_ "github.com/testcontainers/testcontainers-go"
	_ "github.com/testcontainers/testcontainers-go/modules/mysql"
)
