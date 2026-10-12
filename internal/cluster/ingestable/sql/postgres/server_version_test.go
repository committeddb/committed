package postgres

import "testing"

func TestServerMajor(t *testing.T) {
	for in, want := range map[string]int{
		"16.4 (Debian 16.4-1.pgdg120+1)": 16,
		"13.2":                           13,
		"12.18":                          12,
		"17beta1":                        17,
		"":                               0,
		"devel":                          0,
	} {
		if got := serverMajor(in); got != want {
			t.Errorf("serverMajor(%q) = %d, want %d", in, got, want)
		}
	}
}
