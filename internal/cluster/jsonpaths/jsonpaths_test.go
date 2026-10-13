package jsonpaths

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/PaesslerAG/jsonpath"
)

func TestGetMatchesJSONPathGet(t *testing.T) {
	doc := map[string]any{
		"id":    "o-1",
		"data":  map[string]any{"qty": 2.0, "tags": []any{"a", "b"}},
		"items": []any{map[string]any{"sku": "pen"}, map[string]any{"sku": "ink"}},
	}
	paths := []string{
		"$.id",
		"$.data.qty",
		"$.data.tags[1]",
		"$.items[*].sku",
		"$..sku",
		`$.items[?(@.sku == "ink")].sku`,
		"$.missing",
		"$.data.qty.deeper",
		"$.items[",
		"not a path",
	}
	for _, p := range paths {
		for range 2 { // the second call is served from the cache
			want, wantErr := jsonpath.Get(p, doc)
			got, gotErr := Get(p, doc)
			if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("%s: error %v, want %v", p, gotErr, wantErr)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: %v, want %v", p, got, want)
			}
		}
	}
}

func TestGetConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				doc := map[string]any{"v": float64(g*1000 + i)}
				got, err := Get("$.v", doc)
				if err != nil || got != doc["v"] {
					t.Errorf("got %v, %v; want %v", got, err, doc["v"])
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestCacheIsBounded(t *testing.T) {
	for i := range maxCached + 100 {
		if _, err := Get(fmt.Sprintf("$.f%d", i), map[string]any{}); err == nil {
			t.Fatal("expected unresolved-key error")
		}
	}
	if n := size.Load(); n > maxCached {
		t.Fatalf("cache holds %d paths, bound is %d", n, maxCached)
	}
	n := 0
	cache.Range(func(_, _ any) bool { n++; return true })
	if n > maxCached {
		t.Fatalf("cache holds %d paths, bound is %d", n, maxCached)
	}
}
