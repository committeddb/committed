package sql

import (
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// The self-enforcing half of the jsonColumns lesson: the flat/topics
// mutual-exclusivity guard must cover EVERY per-topic field, and the
// per-topic fields ARE topicSpecTOML's mapstructure tags. This test
// derives both sets and requires them equal — adding a field to the
// struct without extending the guard (the exact omission that silently
// swallowed jsonColumns hints in the field) fails CI with this message
// instead of shipping an accepted-and-ignored spelling.
func TestFlatPerTopicGuardCoversEveryField(t *testing.T) {
	var structFields []string
	rt := reflect.TypeOf(topicSpecTOML{})
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("mapstructure")
		require.NotEmpty(t, tag, "every topicSpecTOML field carries a mapstructure tag")
		structFields = append(structFields, "sql."+tag)
	}
	guard := append([]string(nil), flatPerTopicFields...)
	sort.Strings(structFields)
	sort.Strings(guard)
	require.Equal(t, structFields, guard,
		"topicSpecTOML and flatPerTopicFields drifted — a per-topic field missing from the guard is ACCEPTED AND SILENTLY IGNORED at the flat position (the jsonColumns saga); add it to the guard list")
}
