// Package jsonpaths evaluates JSONPaths, compiling each distinct path once.
//
// jsonpath.Get compiles its path on every call. Projections and stages resolve
// the same few config-supplied paths for every input row, and an aggregate
// stage does so for every retained input on every refold, so compiling
// dominated their cost. A compiled path keeps no state between calls, so one
// is shared by every goroutine.
//
// It is a leaf package (it imports no committed code) so the stage runtime,
// the SQL syncables, and loopback share one cache.
package jsonpaths

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
)

// maxCached bounds the cache. Paths come from configs, so a node normally
// holds far fewer; reaching the bound means configs churned, and the cache
// starts over.
const maxCached = 1 << 14

type compiled struct {
	eval gval.Evaluable
	err  error
}

var (
	cache sync.Map // path → compiled
	size  atomic.Int64
)

// Get returns the same value and error as jsonpath.Get(path, value).
func Get(path string, value any) (any, error) {
	c := lookup(path)
	if c.err != nil {
		return nil, c.err
	}
	return c.eval(context.Background(), value)
}

func lookup(path string) compiled {
	if v, ok := cache.Load(path); ok {
		return v.(compiled)
	}
	eval, err := jsonpath.New(path)
	c := compiled{eval: eval, err: err}
	if _, loaded := cache.LoadOrStore(path, c); !loaded && size.Add(1) > maxCached {
		cache.Clear()
		size.Store(0)
	}
	return c
}
