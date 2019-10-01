package syncable

import (
	_ "github.com/go-sql-driver/mysql"    // mysql driver
	_ "github.com/proullon/ramsql/driver" // ramsql driver
)

// Database represents a query database
//counterfeiter:generate . Database
type Database interface {
	Init() error
	Type() string
	Close() error
}
