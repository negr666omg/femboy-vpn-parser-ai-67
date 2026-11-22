package app

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/negr666omg/subwatch/internal/sub"
)

func TestStabilityTrackerRequiresThreeConsecutiveFastURLChecks(t *testing.T) {
	tracker := NewStabilityTracker()
	node := sub.Node{Key: "node-a", URLPassed: true, URLLatencyMS: 99}

	if got := tracker.recordCycle([]sub.Node{node})["node-a"]; got != 1 {
		t.Fatalf("first qualifying cycle streak = %d, want 1", got)
	}
	if got := tracker.recordCycle([]sub.Node{node})["node-a"]; got != 2 {
		t.Fatalf("second qualifying cycle streak = %d, want 2", got)
	}
	if got := tracker.recordCycle([]sub.Node{node})["node-a"]; got != 3 {
		t.Fatalf("third qualifying cycle streak = %d, want 3", got)
	}
}

func TestStabilityTrackerResetsOnFailureSlowPingOrMissingNode(t *testing.T) {
	tracker := NewStabilityTracker()
	fast := sub.Node{Key: "node-a", URLPassed: true, URLLatencyMS: 42}
	failed := sub.Node{Key: "node-a", URLPassed: false, URLLatencyMS: 0}
	slow := sub.Node{Key: "node-a", URLPassed: true, URLLatencyMS: 100}

	tracker.recordCycle([]sub.Node{fast})
	tracker.recordCycle([]sub.Node{fast})
	if got := tracker.recordCycle([]sub.Node{fast})["node-a"]; got != 3 {
		t.Fatalf("expected three-cycle stable streak before reset, got %d", got)
	}
	if got := tracker.recordCycle([]sub.Node{failed})["node-a"]; got != 0 {
		t.Fatalf("failed URL check streak = %d, want 0", got)
	}
	if got := tracker.recordCycle([]sub.Node{fast})["node-a"]; got != 1 {
		t.Fatalf("first fast cycle after failure = %d, want 1", got)
	}
	if got := tracker.recordCycle([]sub.Node{slow})["node-a"]; got != 0 {
		t.Fatalf("100 ms ping streak = %d, want 0", got)
	}
	if got := tracker.recordCycle([]sub.Node{fast})["node-a"]; got != 1 {
		t.Fatalf("first fast cycle after slow ping = %d, want 1", got)
	}
	if streaks := tracker.recordCycle(nil); len(streaks) != 0 {
		t.Fatalf("missing node should reset the history, got %#v", streaks)
	}
	if got := tracker.recordCycle([]sub.Node{fast})["node-a"]; got != 1 {
		t.Fatalf("reappearing node should restart at 1, got %d", got)
	}
}

func TestQualityMarkersAreExclusiveAndGamingTakesPriority(t *testing.T) {
	const base = "🇩🇪 Germany 29↓42↑"
	cases := []struct {
		name string
		stable bool
		gaming bool
		want string
	}{
		{name: base, stable: true, want: base + stabilityNameSuffix},
		{name: base, stable: true, gaming: true, want: base + gamingNameSuffix},
		{name: base + stabilityNameSuffix, stable: true, gaming: true, want: base + gamingNameSuffix},
		{name: base + gamingNameSuffix + stabilityNameSuffix, stable: true, want: base + stabilityNameSuffix},
		{name: base + gamingNameSuffix + geminiNameSuffix, stable: true, want: base + stabilityNameSuffix + geminiNameSuffix},
		{name: base + gamingNameSuffix, want: base},
	}
	for _, tc := range cases {
		if got := qualityMarkedName(tc.name, tc.stable, tc.gaming); got != tc.want {
			t.Errorf("qualityMarkedName(%q, %v, %v) = %q, want %q", tc.name, tc.stable, tc.gaming, got, tc.want)
		}
	}
}

func TestStabilityMarkerIsPlacedBeforeGeminiAndRemovedWhenUnstable(t *testing.T) {
	const base = "🇩🇪 Germany 29↓42↑"
	cases := []struct {
		name   string
		stable bool
		want   string
	}{
		{name: base, stable: true, want: base + " 🧱"},
		{name: base + geminiNameSuffix, stable: true, want: base + " 🧱" + geminiNameSuffix},
		{name: base + " 🧱" + geminiNameSuffix, stable: false, want: base + geminiNameSuffix},
		{name: base + " 🧱", stable: true, want: base + " 🧱"},
		{name: base + " 🧱 🧱", stable: true, want: base + " 🧱"},
	}
	for _, tc := range cases {
		if got := qualityMarkedName(tc.name, tc.stable, false); got != tc.want {
			t.Errorf("qualityMarkedName(%q, %v, false) = %q, want %q", tc.name, tc.stable, got, tc.want)
		}
	}
}

func TestWriteStabilityResultsUpdatesAllSubscriptionLists(t *testing.T) {
	dir := t.TempDir()
	node := sub.Node{
		Key: "node-a", Protocol: "vless", Name: "🇩🇪 Germany 29↓42↑" + geminiNameSuffix,
		URI: "vless://user@example.test:443#test",
	}
	all := []sub.Node{node}
	urlPassed := []sub.Node{node}
	speedPassed := []sub.Node{node}
	gemini := []sub.Node{node}

	if err := writeStabilityResults(dir, all, urlPassed, speedPassed, gemini, map[string]int{"node-a": 3}, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"all.txt", "url-tested.txt", "url-tested-vless.txt", "speedtested.txt", "speedtested-vless.txt"} {
		remark := readSubscriptionRemark(t, filepath.Join(dir, name))
		if !strings.Contains(remark, "🧱") || !strings.Contains(remark, "Gemini") {
			t.Errorf("%s did not get both markers: %q", name, remark)
		}
		if strings.Index(remark, "🧱") > strings.Index(remark, "Gemini") {
			t.Errorf("%s placed the stability marker after Gemini: %q", name, remark)
		}
	}

	if err := writeStabilityResults(dir, all, urlPassed, speedPassed, gemini, map[string]int{"node-a": 3}, map[string]bool{"node-a": true}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"all.txt", "url-tested.txt", "url-tested-vless.txt", "speedtested.txt", "speedtested-vless.txt"} {
		remark := readSubscriptionRemark(t, filepath.Join(dir, name))
		if !strings.Contains(remark, "🎮") || strings.Contains(remark, "🧱") {
			t.Errorf("%s did not give gaming priority over stability: %q", name, remark)
		}
	}

	if err := writeStabilityResults(dir, all, urlPassed, speedPassed, gemini, map[string]int{}, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	remark := readSubscriptionRemark(t, filepath.Join(dir, "all.txt"))
	if strings.Contains(remark, "🧱") {
		t.Fatalf("stale stability marker remained after streak reset: %q", remark)
	}
}

func readSubscriptionRemark(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	uri, err := url.Parse(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("parse URI in %s: %v", path, err)
	}
	return uri.Fragment
}
