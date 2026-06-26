package mysql

import (
	"reflect"
	"testing"
)

// TestParseEnumSetMembers locks the ENUM/SET member parse (no container needed).
// It mirrors go-mysql/canal's naive split so decoded labels stay byte-identical;
// the last case documents that a comma inside a member splits it — a known
// limitation shared with upstream, asserted so any future "fix" is deliberate.
func TestParseEnumSetMembers(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		prefix     string
		want       []string
	}{
		{"enum three", "enum('red','green','blue')", "enum(", []string{"red", "green", "blue"}},
		{"set three", "set('a','b','c')", "set(", []string{"a", "b", "c"}},
		{"enum one member", "enum('only')", "enum(", []string{"only"}},
		{"member with a space", "enum('a b','c')", "enum(", []string{"a b", "c"}},
		{"naive: comma inside a member splits (matches go-mysql)", "enum('a,b','c')", "enum(", []string{"a", "b", "c"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseEnumSetMembers(tc.columnType, tc.prefix)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}
