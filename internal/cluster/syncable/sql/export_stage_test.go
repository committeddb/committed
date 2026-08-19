package sql

// SetStoreDirForTest injects the stage-store directory a parser would
// thread in production (external-package tests cannot reach the
// unexported field).
func SetStoreDirForTest(p *Projection, dir string) { p.storeDir = dir }
