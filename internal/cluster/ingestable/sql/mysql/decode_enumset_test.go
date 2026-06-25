package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
)

// TestDecodeEnumSet exercises the ENUM/SET label resolution directly (no MySQL
// container needed), covering the index/bitmask math and the edge cases the
// binlog can produce: the empty/invalid-enum sentinel (0), out-of-range guard,
// NULL, and an already-text value.
func TestDecodeEnumSet(t *testing.T) {
	enumCol := schema.TableColumn{Type: schema.TYPE_ENUM, EnumValues: []string{"red", "green", "blue"}}
	setCol := schema.TableColumn{Type: schema.TYPE_SET, SetValues: []string{"a", "b", "c"}}
	textCol := schema.TableColumn{Type: schema.TYPE_STRING}

	tests := []struct {
		name string
		col  schema.TableColumn
		in   any
		want any
	}{
		{"enum index resolves to label", enumCol, int64(2), "green"},
		{"enum first member", enumCol, int64(1), "red"},
		{"enum zero is the empty sentinel", enumCol, int64(0), ""},
		{"enum out of range is guarded", enumCol, int64(99), ""},
		{"enum null passes through", enumCol, nil, nil},
		{"enum already-text passes through", enumCol, []byte("green"), []byte("green")},
		{"set bitmask resolves to labels in definition order", setCol, int64(5), "a,c"}, // a(1)|c(4)
		{"set single member", setCol, int64(2), "b"},
		{"set empty mask", setCol, int64(0), ""},
		{"set all members", setCol, int64(7), "a,b,c"},
		{"set null passes through", setCol, nil, nil},
		{"non-enum/set passes through untouched", textCol, "hello", "hello"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := decodeEnumSet(tc.col, tc.in)
			if want, ok := tc.want.([]byte); ok {
				gb, ok := got.([]byte)
				if !ok || string(gb) != string(want) {
					t.Fatalf("got %v, want %s", got, want)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("got %v (%T), want %v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}
