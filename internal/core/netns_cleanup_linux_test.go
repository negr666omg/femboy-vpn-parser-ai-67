//go:build linux

package core

import "testing"

func TestSubWatchResourceNamesAreStrictlyScoped(t *testing.T) {
	for _, name := range []string{"sw-1234abcd", "sw-41789p9eu"} {
		match := subWatchNSPattern.FindStringSubmatch(name)
		if len(match) != 2 || !subWatchIDPattern.MatchString(match[1]) {
			t.Fatalf("valid namespace %q was not recognized", name)
		}
	}
	for _, name := range []string{"sw-", "sw-abc123", "sw-1234;rm", "not-sw-1234", "sw-1234/../other"} {
		if subWatchNSPattern.MatchString(name) {
			t.Fatalf("unsafe namespace %q was recognized", name)
		}
	}
	for _, name := range []string{"swh1234abcd", "swh41789p9eu"} {
		if !subWatchVethPattern.MatchString(name) {
			t.Fatalf("valid veth %q was not recognized", name)
		}
	}
	for _, name := range []string{"swh", "swhabc", "swh1;rm", "other123"} {
		if subWatchVethPattern.MatchString(name) {
			t.Fatalf("unsafe veth %q was recognized", name)
		}
	}
}
