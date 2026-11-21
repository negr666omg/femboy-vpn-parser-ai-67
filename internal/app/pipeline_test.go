package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/negr666omg/subwatch/internal/sub"
)

func TestConfigLoadsStageSpecificSettings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := `{
  "network":{"interface":"throne-tun","isolate_source_downloads":true},
  "sources":{"file":"subs.txt","timeout":"90s"},
  "protocols":{"enabled":["vless","shadowsocks"]},
  "url_test":{"url":"https://example.com/204","concurrency":6,"timeout":"9s"},
  "speed_test":{"provider":"ookla","concurrency":3,"timeout":"45s","first_byte_timeout":"6s","max_duration":"17s","sample_interval":"500ms","min_download_mbps":7,"min_upload_mbps":4,"download_bytes":1048576,"upload_bytes":524288,"download_url":"https://download.example/file?size={bytes}","upload_url":"https://upload.example/test"},
  "service_check":{"concurrency":2,"timeout":"20s","lookup_ip":true,"rename_nodes":true,"geoip_url":"https://geo.example/json","name_format":"{flag} {download}↓{upload}↑","speed_decimals":2},
  "final_check":{"enabled":false,"concurrency":1,"timeout":"8s"},
  "live_display":{"mode":"after_result"},
  "output":{"directory":"results","split_by_protocol":true,"subscription_text":false,"gemini_file":false}
}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := LoadOptions(path)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Interface != "throne-tun" || !opts.IsolateSourceDownloads || filepath.Base(opts.Sources) != "subs.txt" || filepath.Dir(opts.Sources) != dir || opts.OutDir != "results" {
		t.Fatalf("unexpected paths/network: %+v", opts)
	}
	if opts.URLConcurrency != 6 || opts.SpeedConcurrency != 3 || opts.ServiceConcurrency != 2 {
		t.Fatalf("unexpected concurrency: %+v", opts)
	}
	if opts.SourceTimeout != 90*time.Second || opts.URLTimeout != 9*time.Second || opts.SpeedTimeout != 45*time.Second || opts.ServiceTimeout != 20*time.Second || opts.FirstByteTimeout != 0 || opts.MaxSpeedDuration != 17*time.Second || opts.SpeedSampleInterval != 0 {
		t.Fatalf("unexpected timeouts: %+v", opts)
	}
	if len(opts.Protocols) != 2 || opts.Protocols[1] != "ss" {
		t.Fatalf("protocol aliases not normalized: %#v", opts.Protocols)
	}
	if opts.MinDownload != 7 || opts.MinUpload != 4 || !opts.LookupIP || !opts.RenameNodes || opts.GeoIPURL != "https://geo.example/json" || opts.NameFormat != "{flag} {download}↓{upload}↑" || opts.GeminiFile {
		t.Fatalf("unexpected stage settings: %+v", opts)
	}
	if opts.LiveDisplayMode != "after_result" { t.Fatalf("live display mode = %q, want after_result", opts.LiveDisplayMode) }
	if opts.SpeedProvider != "ookla" { t.Fatalf("speed provider = %q, want ookla", opts.SpeedProvider) }
	if opts.DownloadBytes != 0 || opts.UploadBytes != 0 || opts.SpeedDownloadURL != "" || opts.SpeedUploadURL != "" {
		t.Fatalf("Cloudflare-only settings must be disabled for Ookla: %+v", opts)
	}
}

func TestSourceTimeoutMustBePositive(t *testing.T) {
	for _, value := range []string{"0s", "-1s", "invalid"} {
		t.Run(value, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.json")
			data := `{"sources":{"timeout":"` + value + `"}}`
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadOptions(path); err == nil || !strings.Contains(err.Error(), "sources.timeout") {
				t.Fatalf("expected invalid sources.timeout error, got %v", err)
			}
		})
	}
}

func TestDefaultSourceTimeout(t *testing.T) {
	if got := DefaultOptions().SourceTimeout; got != 30*time.Second {
		t.Fatalf("default source timeout = %s, want 30s", got)
	}
}

func TestDefaultStabilityPingThreshold(t *testing.T) {
	if got := DefaultOptions().StabilityMaxPingMS; got != 100 {
		t.Fatalf("default stability max ping = %d, want 100 ms", got)
	}
}

func TestLoadConfiguredStabilityPingThreshold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"stability":{"max_ping_ms":75}}`), 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := LoadOptions(path)
	if err != nil {
		t.Fatal(err)
	}
	if opts.StabilityMaxPingMS != 75 {
		t.Fatalf("configured stability max ping = %d, want 75", opts.StabilityMaxPingMS)
	}
}

func TestInvalidStabilityPingThresholdIsRejected(t *testing.T) {
	for _, value := range []string{"0", "-1"} {
		t.Run(value, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.json")
			if err := os.WriteFile(path, []byte(`{"stability":{"max_ping_ms":`+value+`}}`), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadOptions(path); err == nil || !strings.Contains(err.Error(), "stability.max_ping_ms") {
				t.Fatalf("expected invalid stability.max_ping_ms error, got %v", err)
			}
		})
	}
}

func TestDefaultLiveDisplayModeKeepsCompletedServerVisible(t *testing.T) {
	if got := DefaultOptions().LiveDisplayMode; got != "after_result" {
		t.Fatalf("default live display mode = %q, want after_result", got)
	}
}

func TestInvalidLiveDisplayModeIsRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"live_display":{"mode":"sometimes"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOptions(path); err == nil || !strings.Contains(err.Error(), "live_display.mode") {
		t.Fatalf("expected invalid live_display.mode error, got %v", err)
	}
}

func TestDefaultGeminiFileEnabled(t *testing.T) {
	if !DefaultOptions().GeminiFile {
		t.Fatal("separate Gemini output should remain enabled by default")
	}
}

func TestIsolatedSourceDownloadsRequireExplicitInterface(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"network":{"isolate_source_downloads":true}}`), 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := LoadOptions(path)
	if err != nil {
		t.Fatalf("the config should load before CLI overrides are applied: %v", err)
	}
	opts.OutDir = filepath.Join(dir, "out")
	if err := Run(context.Background(), opts, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "network.interface") {
		t.Fatalf("expected explicit interface validation error at run time, got %v", err)
	}
}


func TestFilterHysteria2AlsoIncludesLegacyHysteria(t *testing.T) {
	nodes := []sub.Node{
		{Name: "legacy", Protocol: "hysteria"},
		{Name: "modern", Protocol: "hysteria2"},
	}
	selected := filterProtocols(nodes, []string{"hysteria2"})
	if len(selected) != 2 {
		t.Fatalf("selected %d nodes, want both Hysteria generations", len(selected))
	}
}


func TestWriteFailureReportCombinesAllFailureStages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.txt")
	failures := []failureRecord{
		{Stage: "SOURCE", Reason: "source 2 (subscription): timeout"},
		{Stage: "PARSE", URI: "broken://not-a-proxy", Reason: "invalid or unsupported proxy configuration"},
		{Stage: "URL TEST", Name: "Germany node", Protocol: "vmess", URI: "vmess://uuid@example.test:443", Reason: "start proxy: connection refused", LatencyMS: 8123},
		{Stage: "SPEED TEST", Name: "US node", Protocol: "vless", URI: "vless://uuid@example.org:443", Reason: "download 4 < 8 Mbps"},
		{Stage: "SERVICE CHECK", Name: "Japan node", Protocol: "trojan", URI: "trojan://secret@example.net:443", Reason: "Gemini status: unavailable"},
		{Stage: "RENAME", Name: "France node", Protocol: "ss", URI: "ss://node", Reason: "GeoIP country lookup failed"},
	}
	if err := writeFailureReport(path, "https://www.gstatic.com/generate_204", failures); err != nil {
		t.Fatalf("writeFailureReport: %v", err)
	}
	report := string(mustRead(t, path))
	for _, want := range []string{
		"Total issues: 6", "[1] SOURCE", "[2] PARSE", "broken://not-a-proxy",
		"[3] URL TEST — Germany node (vmess)", "connection refused", "Latency: 8123 ms",
		"[4] SPEED TEST — US node (vless)", "download 4 < 8 Mbps",
		"[5] SERVICE CHECK — Japan node (trojan)", "Gemini status: unavailable",
		"[6] RENAME — France node (ss)", "GeoIP country lookup failed",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("failed report missing %q:\n%s", want, report)
		}
	}
}

func TestWriteFailureReportHandlesNoFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.txt")
	if err := writeFailureReport(path, "https://example.com/204", nil); err != nil {
		t.Fatalf("writeFailureReport: %v", err)
	}
	if report := string(mustRead(t, path)); !strings.Contains(report, "No failures recorded.") {
		t.Fatalf("unexpected empty report: %s", report)
	}
}

func TestDeduplicateAndProtocolFilter(t *testing.T) {
	nodes := []sub.Node{
		{Name: "A", Protocol: "vless", Key: "same"},
		{Name: "B", Protocol: "vless", Key: "same"},
		{Name: "C", Protocol: "ss", Key: "other"},
	}
	unique, duplicates := deduplicate(nodes)
	if duplicates != 1 || len(unique) != 2 {
		t.Fatalf("unique=%d duplicates=%d", len(unique), duplicates)
	}
	selected := filterProtocols(unique, []string{"ss"})
	if len(selected) != 1 || selected[0].Name != "C" {
		t.Fatalf("unexpected protocol selection: %#v", selected)
	}
}

func TestRunStageWaitsForEveryNodeAndCapsConcurrency(t *testing.T) {
	nodes := make([]sub.Node, 17)
	for i := range nodes {
		nodes[i].Name = "node"
	}
	var active, maxActive, completed atomic.Int32
	var out bytes.Buffer
	results := runStage(context.Background(), "TEST", nodes, 3, &out, func(n *sub.Node) bool { return n.URLPassed }, func(_ context.Context, n *sub.Node) {
		now := active.Add(1)
		for {
			old := maxActive.Load()
			if now <= old || maxActive.CompareAndSwap(old, now) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		n.URLPassed = true
		completed.Add(1)
		active.Add(-1)
	})
	if completed.Load() != int32(len(nodes)) {
		t.Fatalf("completed=%d", completed.Load())
	}
	if maxActive.Load() > 3 {
		t.Fatalf("max concurrency=%d", maxActive.Load())
	}
	if len(results) != len(nodes) {
		t.Fatalf("results=%d", len(results))
	}
	for i, n := range results {
		if !n.URLPassed {
			t.Fatalf("node %d was not processed", i)
		}
	}
	if out.Len() == 0 {
		t.Fatal("expected progress output")
	}
}

func TestStageFilesContainOnlyURIs(t *testing.T) {
	dir := t.TempDir()
	nodes := []sub.Node{
		{Name: "test", Protocol: "ss", URI: "ss://example", URLPassed: true, Outbound: map[string]any{"type": "shadowsocks"}},
		{Name: "second", Protocol: "vless", URI: "vless://user@example.test:443"},
	}
	if err := writeStageFiles(dir, "url-tested", nodes, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"url-tested.txt", "url-tested-ss.txt", "url-tested-vless.txt"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if !strings.Contains(line, "://") || strings.HasPrefix(line, "{") || strings.Contains(line, `"uri"`) {
				t.Fatalf("%s is not a URI-only subscription: %q", name, data)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "url-test-finished.json")); !os.IsNotExist(err) {
		t.Fatalf("unexpected JSON artifact, stat error: %v", err)
	}
}

func TestWriteStageFilesRemovesStaleProtocolSplitsWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	stalePath := filepath.Join(dir, "all-vless.txt")
	if err := os.WriteFile(stalePath, []byte("vless://stale"), 0600); err != nil {
		t.Fatal(err)
	}
	nodes := []sub.Node{{Name: "fresh", Protocol: "ss", URI: "ss://fresh"}}
	if err := writeStageFiles(dir, "all", nodes, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale protocol split still exists: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "all.txt"))
	if err != nil || string(data) != "ss://fresh\n" {
		t.Fatalf("unexpected aggregate output: %q, %v", data, err)
	}
}

func TestRemoveLegacyJSONArtifacts(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"all.json", "final.json", "final-vless.json", "final-personal.json", "report.csv.json", "keep.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeLegacyJSONArtifacts(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"all.json", "final.json", "final-vless.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("legacy artifact %s was not removed", name)
		}
	}
	for _, name := range []string{"final-personal.json", "report.csv.json", "keep.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("unrelated file %s was removed: %v", name, err)
		}
	}
}

func TestSpeedThresholdRequiresBothDirections(t *testing.T) {
	if got := speedThresholdError(12, 4.9, 5, 5); got == "" {
		t.Fatal("upload below threshold must fail")
	}
	if got := speedThresholdError(4.9, 12, 5, 5); got == "" {
		t.Fatal("download below threshold must fail")
	}
	if got := speedThresholdError(5, 5, 5, 5); got != "" {
		t.Fatalf("exact threshold should pass: %s", got)
	}
	if got := speedThresholdError(1, 50, 8, 8); got == "" || !strings.Contains(got, "download") {
		t.Fatalf("download 1 Mbps must fail even when upload is 50 Mbps: %q", got)
	}
	if got := speedThresholdError(50, 1, 8, 8); got == "" || !strings.Contains(got, "upload") {
		t.Fatalf("upload 1 Mbps must fail even when download is 50 Mbps: %q", got)
	}
}

func TestStabilityStateIntersectsEveryCompletedCycle(t *testing.T) {
	state := NewStabilityState()
	if err := state.prepareOutput(t.TempDir()); err != nil {
		t.Fatal(err)
	}

	cycle1URL := []sub.Node{
		{Key: "a", URLPassed: true, URLLatencyMS: 99},
		{Key: "b", URLPassed: true, URLLatencyMS: 100},
		{Key: "c", URLPassed: true, URLLatencyMS: 35},
	}
	cycle1Speed := []sub.Node{
		{Key: "a", URLPassed: true, URLLatencyMS: 99, SpeedPassed: true, DownloadMbps: 50, UploadMbps: 20},
		{Key: "c", URLPassed: true, URLLatencyMS: 35, SpeedPassed: true, DownloadMbps: 1, UploadMbps: 50},
	}
	url, speed, cycle := state.advance(cycle1URL, cycle1Speed, 100, 8, 8)
	if cycle != 1 || len(url) != 2 || len(speed) != 1 || nodeDedupKey(speed[0]) != "a" {
		t.Fatalf("cycle 1 stable URL/speed = %d/%d cycle=%d; want 2/1 cycle=1", len(url), len(speed), cycle)
	}

	cycle2URL := []sub.Node{
		{Key: "a", URLPassed: true, URLLatencyMS: 40},
		{Key: "b", URLPassed: true, URLLatencyMS: 35},
		{Key: "c", URLPassed: true, URLLatencyMS: 101},
	}
	cycle2Speed := []sub.Node{
		{Key: "a", URLPassed: true, URLLatencyMS: 40, SpeedPassed: true, DownloadMbps: 10, UploadMbps: 8},
		{Key: "b", URLPassed: true, URLLatencyMS: 35, SpeedPassed: true, DownloadMbps: 80, UploadMbps: 80},
	}
	url, speed, cycle = state.advance(cycle2URL, cycle2Speed, 100, 8, 8)
	if cycle != 2 || len(url) != 1 || nodeDedupKey(url[0]) != "a" || len(speed) != 1 || nodeDedupKey(speed[0]) != "a" {
		t.Fatalf("cycle 2 stable URL/speed = %#v/%#v cycle=%d; want only a in both", stabilityNodeKeys(url), stabilityNodeKeys(speed), cycle)
	}

	cycle3URL := []sub.Node{{Key: "a", URLPassed: true, URLLatencyMS: 100}}
	cycle3Speed := []sub.Node{{Key: "a", URLPassed: true, URLLatencyMS: 100, SpeedPassed: true, DownloadMbps: 80, UploadMbps: 80}}
	url, speed, cycle = state.advance(cycle3URL, cycle3Speed, 100, 8, 8)
	if cycle != 3 || len(url) != 0 || len(speed) != 0 {
		t.Fatalf("a 100 ms ping must remove the node from both stable lists: URL=%d speed=%d cycle=%d", len(url), len(speed), cycle)
	}
}

func stabilityNodeKeys(nodes []sub.Node) []string {
	keys := make([]string, 0, len(nodes))
	for _, n := range nodes {
		keys = append(keys, nodeDedupKey(n))
	}
	return keys
}

func TestStableMarkerCombinesWithGeminiInOrder(t *testing.T) {
	nodes := []sub.Node{{
		Key: "stable", Protocol: "vless", Name: "🇩🇪 Germany 63↓21↑",
		URI: "vless://uuid@example.test:443#Germany", GeminiStatus: "available",
	}}
	if err := markStableNodes(nodes, nodes); err != nil {
		t.Fatal(err)
	}
	if err := markGeminiNodes(nodes); err != nil {
		t.Fatal(err)
	}
	if want := "🇩🇪 Germany 63↓21↑ 🧱 Gemini 🐱"; nodes[0].Name != want {
		t.Fatalf("combined node name = %q, want %q", nodes[0].Name, want)
	}
}

func TestCollectMetadataKeepsProfileHeadersOnlyOnce(t *testing.T) {
	sources := [][]byte{
		[]byte("#profile-title: Main\n#profile-web-page-url: https://example.net\n#announce: renewed\nvless://one"),
		[]byte("#profile-title: Main\n#support-url: https://example.net/help\nss://two"),
	}
	got := collectMetadata(sources)
	if len(got) != 4 {
		t.Fatalf("expected three unique metadata headers, got %#v", got)
	}
}

func TestMissingConfigAndSourcesAreCreatedOnFirstRun(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	opts, err := LoadOptions(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(configPath); err != nil {
		t.Fatalf("config was not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "sources.txt")); err != nil {
		t.Fatalf("sources file was not created: %v", err)
	}
	if opts.MinDownload != 8 || opts.MinUpload != 8 || opts.DownloadBytes != 2<<20 || opts.UploadBytes != 2<<20 {
		t.Fatalf("unexpected fresh-install defaults: %+v", opts)
	}
}

func TestWriteSubscriptionContainsOnlyProxyURIs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "final.txt")
	nodes := []sub.Node{{URI: "vless://user@example.test:443"}, {URI: "ss://credential@example.test:8388"}}
	if err := writeSubscription(path, nodes); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "vless://user@example.test:443\nss://credential@example.test:8388\n"
	if string(got) != want || strings.Contains(string(got), "#profile-") || strings.Contains(string(got), "#announce") {
		t.Fatalf("unexpected subscription content: %q", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0644 {
		t.Fatalf("subscription permissions = %04o, want 0644 for web-server readability", got)
	}
}

func TestFinalCheckStageRemoved(t *testing.T) {
	if strings.Contains(ExampleConfig(), `"final_check"`) {
		t.Fatal("new config must not expose a duplicate final_check stage")
	}
	opts := DefaultOptions()
	if opts.URLConcurrency < 1 || opts.SpeedConcurrency < 1 || opts.ServiceConcurrency < 1 {
		t.Fatalf("invalid defaults: %+v", opts)
	}
}

func TestStageProtocolFilesUseStagePrefixes(t *testing.T) {
	dir := t.TempDir()
	urlNodes := []sub.Node{
		{Protocol: "vless", URI: "vless://url-one"},
		{Protocol: "ss", URI: "ss://url-two"},
	}
	speedNodes := []sub.Node{{Protocol: "vless", URI: "vless://speed-one"}}
	geminiNodes := []sub.Node{{Protocol: "ss", URI: "ss://gemini-one"}}
	for _, item := range []struct {
		base  string
		nodes []sub.Node
	}{
		{"url-tested", urlNodes},
		{"speedtested", speedNodes},
		{"gemini", geminiNodes},
	} {
		if err := writeStageFiles(dir, item.base, item.nodes, true); err != nil {
			t.Fatal(err)
		}
	}
	for name, want := range map[string]string{
		"url-tested-vless.txt": "vless://url-one\n",
		"url-tested-ss.txt": "ss://url-two\n",
		"speedtested.txt": "vless://speed-one\n",
		"speedtested-vless.txt": "vless://speed-one\n",
		"gemini.txt": "ss://gemini-one\n",
		"gemini-ss.txt": "ss://gemini-one\n",
	} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s: got %q, err %v", name, got, err)
		}
	}
	for _, legacy := range []string{"vless.txt", "ss.txt"} {
		if _, err := os.Stat(filepath.Join(dir, legacy)); !os.IsNotExist(err) {
			t.Fatalf("legacy unqualified protocol file %s exists: %v", legacy, err)
		}
	}
	if err := writeStageFiles(dir, "url-tested", []sub.Node{{Protocol: "ss", URI: "ss://only"}}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "url-tested-vless.txt")); !os.IsNotExist(err) {
		t.Fatal("stale URL protocol list remains")
	}
}

func TestGeminiFileToggleSeparatesGeminiSubscriptions(t *testing.T) {
	dir := t.TempDir()
	parsed := []sub.Node{{Key: "same", Protocol: "vless", Name: "old", URI: "vless://node#old"}}
	urlNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "old", URI: "vless://node#old", URLPassed: true}}
	speedNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "🇫🇮 80↓50↑", URI: "vless://node#fast", URLPassed: true, SpeedPassed: true}}
	geminiNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "🇫🇮 80↓50↑", URI: "vless://node#fast", URLPassed: true, SpeedPassed: true, GeminiStatus: "available"}}
	if err := writeStageFiles(dir, "url-tested", urlNodes, true); err != nil {
		t.Fatal(err)
	}
	if err := writeStageFiles(dir, "speedtested", speedNodes, true); err != nil {
		t.Fatal(err)
	}
	if err := writeGeminiResults(dir, geminiNodes, parsed, urlNodes, speedNodes, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"gemini.txt", "gemini-vless.txt"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected separate Gemini file %s: %v", name, err)
		}
	}
	if !strings.HasSuffix(geminiNodes[0].Name, geminiNameSuffix) || !strings.Contains(geminiNodes[0].URI, "Gemini") {
		t.Fatalf("dedicated Gemini list is missing its marker: name=%q uri=%q", geminiNodes[0].Name, geminiNodes[0].URI)
	}
	if !strings.Contains(string(mustRead(t, filepath.Join(dir, "gemini.txt"))), "Gemini") {
		t.Fatal("dedicated Gemini text file is missing its name marker")
	}
	if strings.Contains(string(mustRead(t, filepath.Join(dir, "url-tested.txt"))), "Gemini") {
		t.Fatal("separate Gemini mode modified earlier stage files")
	}
}

func TestGeminiDisabledMarksNodesInEveryApplicableFile(t *testing.T) {
	dir := t.TempDir()
	parsed := []sub.Node{{Key: "same", Protocol: "vless", Name: "old", URI: "vless://node#old"}}
	urlNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "old", URI: "vless://node#old", URLPassed: true}}
	speedNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "🇫🇮 80↓50↑", URI: "vless://node#fast", URLPassed: true, SpeedPassed: true}}
	geminiNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "🇫🇮 80↓50↑", URI: "vless://node#fast", URLPassed: true, SpeedPassed: true, GeminiStatus: "available"}}
	if err := writeGeminiResults(dir, geminiNodes, parsed, urlNodes, speedNodes, false); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"all.txt", "url-tested.txt", "url-tested-vless.txt", "speedtested.txt", "speedtested-vless.txt"} {
		data := string(mustRead(t, filepath.Join(dir, name)))
		if !strings.Contains(data, "Gemini") {
			t.Fatalf("%s did not receive Gemini marker: %q", name, data)
		}
	}
	if !strings.HasSuffix(geminiNodes[0].Name, geminiNameSuffix) {
		t.Fatalf("unexpected marked name: %q", geminiNodes[0].Name)
	}
	for _, name := range []string{"gemini.txt", "gemini-vless.txt", "gemini-available.txt"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("separate Gemini output %s should be absent: %v", name, err)
		}
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func TestRunStageCallbackRunsForEachCompletedNode(t *testing.T) {
	nodes := []sub.Node{{Name: "one"}, {Name: "two"}, {Name: "three"}}
	var mu sync.Mutex
	completed := make([]string, 0, len(nodes))
	results, err := runStageWithCallback(context.Background(), "CALLBACK", nodes, 2, &bytes.Buffer{}, func(*sub.Node) bool { return true }, func(_ context.Context, n *sub.Node) { n.URLPassed = true }, func(n sub.Node) error {
		mu.Lock()
		defer mu.Unlock()
		completed = append(completed, n.Name)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(nodes) || len(completed) != len(nodes) {
		t.Fatalf("results=%d callbacks=%d", len(results), len(completed))
	}
}

func TestCleanOutputArtifactsRemovesOldStagesAndKeepsConfig(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"config.json", "all.txt", "url-tested.txt", "url-tested-vless.txt", "speedtested.txt", "speedtested-vless.txt", "failed.txt", "selected.txt", "url-test-failed.txt", "final-vless.txt", "report.csv", "vless.txt", "gemini.txt", "gemini-available.txt", "speed-test-finished.json", ".subwatch-interrupted.tmp", "notes-old.txt", "custom-export.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := cleanOutputArtifacts(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"all.txt", "url-tested.txt", "url-tested-vless.txt", "speedtested.txt", "speedtested-vless.txt", "failed.txt", "selected.txt", "url-test-failed.txt", "final-vless.txt", "report.csv", "vless.txt", "gemini.txt", "gemini-available.txt", "speed-test-finished.json", ".subwatch-interrupted.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("generated artifact %s remains: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("config was removed: %v", err)
	}
	for _, name := range []string{"notes-old.txt", "custom-export.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("unrelated user file %s was removed: %v", name, err)
		}
	}
}

func TestClearProtocolSubscriptionsPreservesUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"url-tested-vless.txt",
		"url-tested-ss.txt",
		"url-tested-personal-notes.txt",
		"url-tested-vpn-backup.txt",
		"speedtested-vless.txt",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := clearProtocolSubscriptions(dir, "url-tested"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"url-tested-vless.txt", "url-tested-ss.txt"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("generated protocol file %s remains: %v", name, err)
		}
	}
	for _, name := range []string{"url-tested-personal-notes.txt", "url-tested-vpn-backup.txt", "speedtested-vless.txt"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("unrelated file %s was removed: %v", name, err)
		}
	}
}

func TestLoadSourcesSkipsTimedOutSubscriptionAndContinues(t *testing.T) {
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte("vless://too-late@example.test:443"))
	}))
	defer slowServer.Close()

	fastServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("vless://working@example.test:443"))
	}))
	defer fastServer.Close()

	dir := t.TempDir()
	sourcesPath := filepath.Join(dir, "sources.txt")
	contents := slowServer.URL + "\n" + fastServer.URL + "\n"
	if err := os.WriteFile(sourcesPath, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}

	sources, err := loadSourcesWithTimeout(context.Background(), sourcesPath, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("one timed-out source must not abort loading: %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("expected both source entries to be retained, got %d", len(sources))
	}
	if sources[0].LoadError == nil {
		t.Fatal("expected the slow source to be marked as failed")
	}
	if !strings.Contains(strings.ToLower(sources[0].LoadError.Error()), "timeout") && !strings.Contains(strings.ToLower(sources[0].LoadError.Error()), "deadline") {
		t.Fatalf("expected timeout diagnostic, got %v", sources[0].LoadError)
	}
	if sources[0].Line != 1 {
		t.Fatalf("expected original source line 1, got %d", sources[0].Line)
	}
	if sources[1].LoadError != nil {
		t.Fatalf("fast source should still load successfully: %v", sources[1].LoadError)
	}
	if got := string(sources[1].Body); got != "vless://working@example.test:443" {
		t.Fatalf("unexpected body from source after timeout: %q", got)
	}
}

func TestLoadSourcesWithFetcherUsesNamespaceFetcherOnlyForRemoteURLs(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.txt")
	if err := os.WriteFile(localPath, []byte("ss://local-source"), 0600); err != nil {
		t.Fatal(err)
	}
	sourcesPath := filepath.Join(dir, "sources.txt")
	contents := "https://subscription.example/list\n" + localPath + "\nvless://inline@example.test:443\n"
	if err := os.WriteFile(sourcesPath, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	var calls []string
	fetcher := func(_ context.Context, sourceURL string, timeout time.Duration) ([]byte, error) {
		calls = append(calls, sourceURL)
		if timeout != 5*time.Second {
			t.Fatalf("fetch timeout = %s, want 5s", timeout)
		}
		return []byte("vless://remote@example.test:443"), nil
	}
	sources, err := loadSourcesWithFetcher(context.Background(), sourcesPath, 5*time.Second, fetcher)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 3 || len(calls) != 1 || calls[0] != "https://subscription.example/list" {
		t.Fatalf("unexpected fetch calls or sources: calls=%#v sources=%#v", calls, sources)
	}
	if string(sources[0].Body) != "vless://remote@example.test:443" || string(sources[1].Body) != "ss://local-source" || string(sources[2].Body) != "vless://inline@example.test:443" {
		t.Fatalf("source bodies were not loaded correctly: %#v", sources)
	}
}

func TestLoadSourcesReportsPerSourceProgress(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.txt")
	if err := os.WriteFile(localPath, []byte("ss://local-source"), 0600); err != nil {
		t.Fatal(err)
	}
	sourcesPath := filepath.Join(dir, "sources.txt")
	contents := "https://user:secret@subscription.example/private?token=hidden\n" + localPath + "\nvless://id@example.test:443\n"
	if err := os.WriteFile(sourcesPath, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	var progress []sourceProgress
	fetcher := func(_ context.Context, _ string, _ time.Duration) ([]byte, error) {
		return []byte("vless://remote@example.test:443"), nil
	}
	sources, err := loadSourcesWithFetcherProgress(context.Background(), sourcesPath, time.Second, fetcher, func(p sourceProgress) {
		progress = append(progress, p)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 3 {
		t.Fatalf("expected three sources, got %d", len(sources))
	}
	wantPhases := []string{"reading source list", "downloading", "downloaded", "reading local file", "loaded", "loading inline link", "loaded", "all sources loaded"}
	phaseIndex := 0
	for _, event := range progress {
		if phaseIndex < len(wantPhases) && event.Phase == wantPhases[phaseIndex] {
			phaseIndex++
		}
	}
	if phaseIndex != len(wantPhases) {
		t.Fatalf("missing or out-of-order progress events, got %#v", progress)
	}
	last := progress[len(progress)-1]
	if last.Index != 3 || last.Total != 3 || last.Loaded != 3 || last.Skipped != 0 {
		t.Fatalf("unexpected final progress: %+v", last)
	}
}

func TestSourceProgressLineRedactsCredentialsAndFitsTerminal(t *testing.T) {
	progress := sourceProgress{
		Index: 2, Total: 12, Loaded: 1, Skipped: 0,
		Reference: "https://user:secret@subscription.example/private?token=hidden",
		Phase: "downloading",
	}
	if got := sourceProgressTitle(progress.Reference); got != "https://subscription.example/private?token=REDACTED" {
		t.Fatalf("source title should show the full safe URL: %q", got)
	}
	line := formatSourceProgressLine(progress, 120)
	if strings.Contains(line, "secret") || strings.Contains(line, "hidden") || !strings.Contains(line, "/private?token=REDACTED") {
		t.Fatalf("source progress should show the URL path while redacting credentials: %q", line)
	}
	head := "[PARSE 2/12 ok:1 skip:0]"
	startCol, endCol, ok := liveTitleClickRange(line, head)
	if !ok || startCol < 1 || endCol <= startCol {
		t.Fatalf("full source title is not exposed as a clickable range: %d-%d, line=%q", startCol, endCol, line)
	}
	if got := displayWidth(line); got > 119 {
		t.Fatalf("source status width=%d, exceeds terminal limit: %q", got, line)
	}
	progress.Phase = "parsing"
	progress.LinesProcessed, progress.TotalLines, progress.Nodes, progress.Invalid = 256, 1024, 74, 3
	line = formatSourceProgressLine(progress, 120)
	for _, want := range []string{"lines 256/1024", "nodes 74", "bad 3"} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing parser progress %q in %q", want, line)
		}
	}
	line = formatSourceProgressLine(sourceProgress{
		Index: 1, Total: 9, Loaded: 0, Reference: "https://user:secret@subscription.example/private?token=hidden",
		Phase: "downloading",
	}, 80)
	if !strings.Contains(line, "subscription.example") || !strings.Contains(line, "downloading") {
		t.Fatalf("narrow terminal hid the active source or action: %q", line)
	}
}

func TestLoadSourcesDoesNotSwallowWholeRunCancellation(t *testing.T) {
	dir := t.TempDir()
	sourcesPath := filepath.Join(dir, "sources.txt")
	if err := os.WriteFile(sourcesPath, []byte("https://example.test/sub\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := loadSourcesWithTimeout(ctx, sourcesPath, time.Second); err != context.Canceled {
		t.Fatalf("expected explicit cancellation to abort, got %v", err)
	}
}

func TestBuildSubscriptionSourceStatsSeparatesExclusiveAndSharedNodes(t *testing.T) {
	sources := []loadedSource{
		{Reference: "https://source-a.example/sub", Line: 1},
		{Reference: "https://source-b.example/sub", Line: 2},
	}
	perSource := [][]sub.Node{
		{
			{Key: "node-1"},
			{Key: "shared"},
			{Key: "shared"}, // repeated within one source still counts once
			{Key: "node-3"},
		},
		{
			{Key: "shared"},
			{Key: "node-4"},
		},
	}
	urlPassed := []sub.Node{{Key: "node-1"}, {Key: "shared"}, {Key: "node-4"}}
	speedPassed := []sub.Node{{Key: "node-1"}, {Key: "shared"}}
	geminiAvailable := []sub.Node{{Key: "shared"}}

	stats, appearances, globallyUnique, sharedUnique := buildSubscriptionSourceStats(
		sources, perSource, urlPassed, speedPassed, geminiAvailable,
	)
	if appearances != 5 || globallyUnique != 4 || sharedUnique != 1 {
		t.Fatalf("overlap totals = appearances:%d unique:%d shared:%d, want 5/4/1", appearances, globallyUnique, sharedUnique)
	}
	byReference := make(map[string]sourceQualityStats, len(stats))
	for _, item := range stats {
		byReference[item.Reference] = item
	}
	a := byReference[sources[0].Reference]
	if a.Eligible != 3 || a.Exclusive != 2 || a.Shared != 1 ||
		a.ExclusiveURLPassed != 1 || a.SharedURLPassed != 1 ||
		a.ExclusiveSpeedPassed != 1 || a.SharedSpeedPassed != 1 ||
		a.ExclusiveGemini != 0 || a.SharedGemini != 1 {
		t.Fatalf("unexpected source A attribution: %+v", a)
	}
	b := byReference[sources[1].Reference]
	if b.Eligible != 2 || b.Exclusive != 1 || b.Shared != 1 ||
		b.ExclusiveURLPassed != 1 || b.SharedURLPassed != 1 ||
		b.ExclusiveSpeedPassed != 0 || b.SharedSpeedPassed != 1 ||
		b.ExclusiveGemini != 0 || b.SharedGemini != 1 {
		t.Fatalf("unexpected source B attribution: %+v", b)
	}
}

func TestPrintSubscriptionQualitySummaryRanksByExclusiveResultsAndRedactsURLSecrets(t *testing.T) {
	sources := []loadedSource{
		{Reference: "https://user:secret@source-a.example/sub?token=hidden", Line: 1},
		{Reference: "https://source-b.example/sub?token=also-hidden", Line: 2},
	}
	perSource := [][]sub.Node{
		{{Key: "a-only"}, {Key: "shared"}},
		{{Key: "b-only-1"}, {Key: "b-only-2"}, {Key: "shared"}},
	}
	urlPassed := []sub.Node{{Key: "a-only"}, {Key: "b-only-1"}, {Key: "b-only-2"}, {Key: "shared"}}
	var output bytes.Buffer
	printSubscriptionQualitySummary(&output, sources, perSource, urlPassed, nil, nil)
	got := output.String()
	first := strings.Index(got, "1. source-b.example [sources.txt:2]")
	second := strings.Index(got, "2. source-a.example [sources.txt:1]")
	if first < 0 || second <= first {
		t.Fatalf("sources should rank by exclusive URL-passed configs, not copied shared configs:\n%s", got)
	}
	for _, secret := range []string{"user:secret", "token=hidden", "also-hidden"} {
		if strings.Contains(got, secret) {
			t.Fatalf("summary leaked subscription URL secret %q:\n%s", secret, got)
		}
	}
	for _, want := range []string{
		"5 unique source entries -> 4 globally unique",
		"configs: 3 enabled unique | 2 only here | 1 shared",
		"URL:    only here 2/2 (100.0%) | shared 1/1 (100.0%)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary missing %q:\n%s", want, got)
		}
	}
}

func TestLiveStageRendersOneReplaceableStatusLine(t *testing.T) {
	t.Setenv("COLUMNS", "")
	nodes := []sub.Node{
		{Name: "First config", Protocol: "vless"},
		{Name: "Second config", Protocol: "ss"},
	}
	var out bytes.Buffer
	results, err := runStageWithLiveCallback(
		context.Background(), "URL TEST", nodes, 1, &out,
		func(n *sub.Node) bool { return n.URLPassed },
		func(_ context.Context, n *sub.Node, live func(sub.Node, string)) {
			live(*n, "checking URL…")
			n.URLLatencyMS = 42
			n.URLPassed = true
			live(*n, urlLiveDetail(*n))
		}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(nodes) || !results[0].URLPassed || !results[1].URLPassed {
		t.Fatalf("unexpected results: %#v", results)
	}
	output := out.String()
	if got := strings.Count(output, "\n"); got != 1 {
		t.Fatalf("status output should occupy one replaceable terminal line, got %d newlines: %q", got, output)
	}
	if !strings.Contains(output, "\x1b[2K") || !strings.Contains(output, "ping: 42 ms") || !strings.Contains(output, "Second config (ss)") {
		t.Fatalf("live status details missing: %q", output)
	}
}


func TestLiveStageKeepsLastPingWhileNextConfigRuns(t *testing.T) {
	t.Setenv("COLUMNS", "")
	nodes := []sub.Node{
		{Name: "First config", Protocol: "vless"},
		{Name: "Second config", Protocol: "ss"},
	}
	var out bytes.Buffer
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	result := make(chan error, 1)

	go func() {
		_, err := runStageWithLiveCallbackMode(
			context.Background(), "URL TEST", nodes, 1, &out,
			func(n *sub.Node) bool { return n.URLPassed },
			func(_ context.Context, n *sub.Node, live func(sub.Node, string)) {
				if n.Name == "First config" {
					n.URLPassed = true
					n.URLLatencyMS = 42
					live(*n, urlLiveDetail(*n))
					return
				}
				live(*n, "checking URL…")
				close(secondStarted)
				<-releaseSecond
				n.URLPassed = true
				n.URLLatencyMS = 87
				live(*n, urlLiveDetail(*n))
			},
			nil,
			"after_result",
		)
		result <- err
	}()

	<-secondStarted
	output := out.String()
	if !strings.Contains(output, "First config (vless)") || !strings.Contains(output, "ping: 42 ms") || strings.Contains(output, "Second config (ss)") {
		t.Fatalf("previous server's full line should remain while the next server is being tested: %q", output)
	}
	close(releaseSecond)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	output = out.String()
	if !strings.Contains(output, "Second config (ss)") || !strings.Contains(output, "ping: 87 ms") {
		t.Fatalf("new server line should replace the previous one after its result: %q", output)
	}
}


func TestLiveStatusFitsTerminalAndKeepsPingAtRightEdge(t *testing.T) {
	node := sub.Node{
		Name:         "🇩🇪 AetrisVPN — long config name for responsive layout",
		Protocol:     "vless",
		URLPassed:    true,
		URLLatencyMS: 124,
	}
	for _, width := range []int{40, 80, 120, 160} {
		line := formatLiveStatusLine("URL TEST", 222, 640, 8, node, "checking URL and collecting a long diagnostic status", width)
		if !strings.HasSuffix(line, "ping: 124 ms") {
			t.Fatalf("ping is not the final field for width %d: %q", width, line)
		}
		if got, want := displayWidth(line), width-1; got != want {
			t.Fatalf("line width for terminal %d = %d cells, want %d: %q", width, got, want, line)
		}
		if strings.ContainsAny(line, "\r\n") {
			t.Fatalf("status contains a line break: %q", line)
		}
	}
}

func TestLiveStatusShowsUnavailablePingUntilURLPasses(t *testing.T) {
	node := sub.Node{Name: "Failed config", Protocol: "vless", URLLatencyMS: 3065}
	if got := livePingText(node); got != "ping: — ms" {
		t.Fatalf("failed URL attempt must not be presented as a valid ping: %q", got)
	}
	node.URLPassed = true
	if got := livePingText(node); got != "ping: 3065 ms" {
		t.Fatalf("successful URL ping missing milliseconds: %q", got)
	}
}

func TestRetryOnConnectionErrorDefaultsDisabled(t *testing.T) {
	if DefaultOptions().RetryOnConnectionError {
		t.Fatal("network.retry_on_connection_error must default to false")
	}
}

func TestRetryOnConnectionErrorConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"network":{"retry_on_connection_error":true}}`), 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := LoadOptions(path)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.RetryOnConnectionError {
		t.Fatal("network.retry_on_connection_error was not loaded")
	}
}

func TestTransientNetworkErrorClassification(t *testing.T) {
	for _, message := range []string{
		"dial tcp: i/o timeout",
		"ssh: connect to host example.test port 22: Connection timed out",
		"connect: connection refused",
		"network is unreachable",
		"temporary failure in name resolution",
		"download sing-box: HTTP 503 Service Unavailable",
		"no DHCPOFFERS received",
	} {
		if !isTransientNetworkError(errors.New(message)) {
			t.Errorf("expected transient network classification for %q", message)
		}
	}
	for _, message := range []string{
		"SSH private key does not exist",
		"Permission denied (publickey)",
		"sing-box checksum mismatch",
		"server returned HTTP 404",
		"output directory is not writable",
		"invalid url_test.timeout",
	} {
		if isTransientNetworkError(errors.New(message)) {
			t.Errorf("permanent/non-network error must not be retried: %q", message)
		}
	}
	if !isTransientNetworkError(context.DeadlineExceeded) {
		t.Fatal("network deadline should be treated as transient")
	}
}

func TestLoadSourcesMarksTransientRemoteFailures(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.txt")
	if err := os.WriteFile(path, []byte("https://timeout.example/sub"), 0600); err != nil {
		t.Fatal(err)
	}
	sources, err := loadSourcesWithFetcher(context.Background(), path, time.Second, func(context.Context, string, time.Duration) ([]byte, error) {
		return nil, errors.New("dial tcp: i/o timeout")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || !sources[0].TransientConnectionError {
		t.Fatalf("temporary remote failure was not marked retryable: %#v", sources)
	}
	sources, err = loadSourcesWithFetcher(context.Background(), path, time.Second, func(context.Context, string, time.Duration) ([]byte, error) {
		return nil, errors.New("server returned HTTP 404")
	})
	if err != nil {
		t.Fatal(err)
	}
	if sources[0].TransientConnectionError {
		t.Fatal("HTTP 404 must not be retried as a transient network failure")
	}
}

func TestSourceRetryWaitStopsOnCancellation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.txt")
	if err := os.WriteFile(path, []byte("https://timeout.example/sub"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	sources, err := loadSourcesWithFetcherProgress(ctx, path, time.Second,
		func(context.Context, string, time.Duration) ([]byte, error) {
			calls++
			return nil, errors.New("dial tcp: i/o timeout")
		},
		func(progress sourceProgress) {
			if progress.Phase == "connection unavailable; retrying in 5s" {
				cancel()
			}
		},
		true,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation during retry wait, got %v (sources=%#v)", err, sources)
	}
	if calls != 1 {
		t.Fatalf("fetch attempts before cancellation = %d, want 1", calls)
	}
}

func TestAllSourcesFailedWithTransientConnections(t *testing.T) {
	sources := []loadedSource{
		{Reference: "https://one.example/sub", LoadError: errors.New("timeout"), TransientConnectionError: true},
		{Reference: "http://two.example/sub", LoadError: errors.New("connection refused"), TransientConnectionError: true},
	}
	if !allSourcesFailedWithTransientConnections(sources) {
		t.Fatal("all remote sources with transient errors should be retryable")
	}
	sources[0].Reference = "./local.txt"
	if allSourcesFailedWithTransientConnections(sources) {
		t.Fatal("local file failures must not be classified as remote connection failures")
	}
	sources[0].Reference = "https://one.example/sub"
	sources[1].TransientConnectionError = false
	if allSourcesFailedWithTransientConnections(sources) {
		t.Fatal("permanent HTTP/source failures must not be retried")
	}
}

func TestRetryableConnectionErrorIsRecognizedThroughWrapping(t *testing.T) {
	err := newRetryableConnectionError("fetch sources", errors.New("timeout"))
	if !IsRetryableConnectionError(fmt.Errorf("run: %w", err)) {
		t.Fatal("wrapped retryable connection error was not recognized")
	}
	if IsRetryableConnectionError(errors.New("permission denied")) {
		t.Fatal("permanent error was incorrectly recognized as retryable")
	}
}

func TestRemoteUploadConfigAndDefaults(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	config := `{
  "network": {"retry_on_connection_error": true},
  "remote_upload": {
    "enabled": true,
    "method": "sftp",
    "host": "192.0.2.10",
    "port": 2222,
    "username": "ubuntu",
    "auth": "private_key",
    "private_key": "~/.ssh/id_ed25519",
    "remote_directory": "/home/ubuntu/results"
  }
}`
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := LoadOptions(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.RemoteUpload.Enabled || opts.RemoteUpload.Method != "sftp" || opts.RemoteUpload.Port != 2222 || opts.RemoteUpload.Auth != "private_key" || !opts.RemoteUpload.RetryOnConnectionError {
		t.Fatalf("unexpected remote upload options: %+v", opts.RemoteUpload)
	}
	if opts.RemoteUpload.PrivateKey == "~/.ssh/id_ed25519" {
		t.Fatal("private key home shortcut was not expanded")
	}
}

func TestMergeResultsKeepsUpdatedURIAfterRename(t *testing.T) {
	original := sub.Node{URI: "vless://example.test:443#old", Key: "stable-key", Name: "old"}
	renamed := original
	renamed.URI = "vless://example.test:443#🇩🇪%2070%E2%86%9334%E2%86%91"
	renamed.Name = "🇩🇪 70↓34↑"
	got := mergeResults([]sub.Node{original}, []sub.Node{original}, []sub.Node{renamed}, []sub.Node{renamed})
	if len(got) != 1 || got[0].URI != renamed.URI || got[0].Name != renamed.Name {
		t.Fatalf("renamed URI was lost during merge: %#v", got)
	}
}

func TestFormatMbpsRoundsToWholeNumbers(t *testing.T) {
	cases := []struct{ value float64; want string }{
		{70, "70"}, {70.4, "70"}, {70.5, "71"}, {0.4, "0"}, {0.5, "1"}, {0.55, "1"},
	}
	for _, tc := range cases {
		if got := formatMbps(tc.value); got != tc.want { t.Errorf("formatMbps(%v)=%q want %q", tc.value, got, tc.want) }
	}
}


func TestInvalidConfigsAreIncludedInFailedReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.txt")
	failures := []failureRecord{
		{Stage: "PARSE", URI: "vless://missing-port", Reason: "invalid or unsupported proxy configuration"},
		{Stage: "PARSE", URI: "unsupported://example.test:443", Reason: "invalid or unsupported proxy configuration"},
	}
	if err := writeFailureReport(path, "https://example.com/204", failures); err != nil {
		t.Fatal(err)
	}
	report := string(mustRead(t, path))
	for _, want := range []string{"PARSE", "vless://missing-port", "unsupported://example.test:443"} {
		if !strings.Contains(report, want) {
			t.Fatalf("failed.txt missing %q: %s", want, report)
		}
	}
}


func TestLiveStageRendersOneStatusRowPerConcurrentWorker(t *testing.T) {
	t.Setenv("COLUMNS", "120")
	nodes := []sub.Node{
		{Name: "First config", Protocol: "vless"},
		{Name: "Second config", Protocol: "vmess"},
	}
	var out bytes.Buffer
	started := make(chan struct{}, len(nodes))
	release := make(chan struct{})
	resultsCh := make(chan []sub.Node, 1)
	errCh := make(chan error, 1)

	go func() {
		results, err := runStageWithLiveCallback(
			context.Background(), "URL TEST", nodes, 2, &out,
			func(n *sub.Node) bool { return n.URLPassed },
			func(_ context.Context, n *sub.Node, live func(sub.Node, string)) {
				live(*n, "checking URL…")
				started <- struct{}{}
				<-release
				n.URLPassed = true
				n.URLLatencyMS = 42
				live(*n, urlLiveDetail(*n))
			}, nil,
		)
		resultsCh <- results
		errCh <- err
	}()

	<-started
	<-started
	close(release)
	results := <-resultsCh
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !results[0].URLPassed || !results[1].URLPassed {
		t.Fatalf("unexpected results: %#v", results)
	}
	output := out.String()
	if !strings.Contains(output, "First config") || !strings.Contains(output, "Second config") {
		t.Fatalf("concurrent worker rows did not both appear: %q", output)
	}
	if !strings.Contains(output, "[URL TEST worker 1]") || !strings.Contains(output, "[URL TEST worker 2]") {
		t.Fatalf("worker row labels are missing: %q", output)
	}
	if !strings.Contains(output, "\x1b[3A") {
		t.Fatalf("expected a three-row live block (summary + two workers) to be redrawn: %q", output)
	}
}


func TestDefaultSpeedTestProviderIsCloudflare(t *testing.T) {
	if got := DefaultOptions().SpeedProvider; got != "cloudflare" {
		t.Fatalf("default speed provider = %q, want cloudflare", got)
	}
}

func TestInvalidSpeedTestProviderIsRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"speed_test":{"provider":"not-a-provider"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOptions(path); err == nil || !strings.Contains(err.Error(), "speed_test.provider") {
		t.Fatalf("expected speed_test.provider validation error, got %v", err)
	}
}


func TestOoklaIgnoresCloudflareOnlyConfigFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := `{"speed_test":{"provider":"ookla","timeout":"70s","max_duration":"18s","first_byte_timeout":"not-a-duration","sample_interval":"also-invalid","download_bytes":-10,"upload_bytes":-20,"download_url":"https://ignored.example/down","upload_url":"https://ignored.example/up","min_download_mbps":6,"min_upload_mbps":3}}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := LoadOptions(path)
	if err != nil {
		t.Fatalf("Ookla must ignore Cloudflare-only settings: %v", err)
	}
	if opts.SpeedProvider != "ookla" || opts.SpeedTimeout != 70*time.Second || opts.MaxSpeedDuration != 18*time.Second {
		t.Fatalf("common Ookla settings were not retained: %+v", opts)
	}
	if opts.FirstByteTimeout != 0 || opts.SpeedSampleInterval != 0 || opts.DownloadBytes != 0 || opts.UploadBytes != 0 || opts.SpeedDownloadURL != "" || opts.SpeedUploadURL != "" {
		t.Fatalf("Cloudflare-only settings were not disabled: %+v", opts)
	}
	if opts.MinDownload != 6 || opts.MinUpload != 3 {
		t.Fatalf("shared speed thresholds were lost: %.1f/%.1f", opts.MinDownload, opts.MinUpload)
	}
}

func TestCloudflareKeepsProviderSpecificConfigFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := `{"speed_test":{"provider":"cloudflare","first_byte_timeout":"7s","sample_interval":"300ms","download_bytes":1048576,"upload_bytes":524288,"download_url":"https://down.example/{bytes}","upload_url":"https://up.example/test"}}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := LoadOptions(path)
	if err != nil {
		t.Fatal(err)
	}
	if opts.FirstByteTimeout != 7*time.Second || opts.SpeedSampleInterval != 300*time.Millisecond {
		t.Fatalf("Cloudflare timings were not loaded: %+v", opts)
	}
	if opts.DownloadBytes != 1048576 || opts.UploadBytes != 524288 || opts.SpeedDownloadURL != "https://down.example/{bytes}" || opts.SpeedUploadURL != "https://up.example/test" {
		t.Fatalf("Cloudflare payload settings were not loaded: %+v", opts)
	}
}

func TestURLStageExportsCountryNameWithoutUnmeasuredSpeed(t *testing.T) {
	node, err := sub.ParseURI("vless://11111111-1111-4111-8111-111111111111@example.test:443#original")
	if err != nil {
		t.Fatal(err)
	}
	name := urlStageNodeName("DE")
	if name != "🇩🇪 Germany" {
		t.Fatalf("URL-stage name = %q, want flag and country name", name)
	}
	if err := sub.RenameNode(&node, name); err != nil {
		t.Fatal(err)
	}
	node.URLPassed = true

	dir := t.TempDir()
	if err := writeStageFiles(dir, "url-tested", []sub.Node{node}, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "url-tested-vless.txt"))
	if err != nil {
		t.Fatal(err)
	}
	exported, err := sub.ParseURI(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("parse exported URL-stage subscription: %v", err)
	}
	if exported.Name != "🇩🇪 Germany" {
		t.Fatalf("exported URL-stage name = %q", exported.Name)
	}
	if strings.Contains(exported.Name, "↓") || strings.Contains(exported.Name, "↑") {
		t.Fatalf("URL-stage name contains speed that has not been measured: %q", exported.Name)
	}
}

func TestNodeNameAddsCountryToExistingCustomTemplate(t *testing.T) {
	// Existing config.json files may still use the old template. It should
	// automatically gain the country placeholder without user edits.
	got := renderNodeName("{flag} {download}↓{upload}↑", "DE", 72.4, 31.6)
	want := "🇩🇪 Germany 72↓32↑"
	if got != want {
		t.Fatalf("renderNodeName() = %q, want %q", got, want)
	}
}

func TestCountryLookupParsesConfiguredGeoIPProvider(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/json" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"country_code":"NL","country":"Netherlands"}`))
	}))
	defer server.Close()

	got := lookupCountry(context.Background(), server.Client(), server.URL+"/json")
	if got != "NL" {
		t.Fatalf("lookupCountry() = %q, want NL", got)
	}
}

func TestCountryLabelSupportsCommonExitRegions(t *testing.T) {
	tests := []struct {
		code, want string
	}{
		{"DE", "🇩🇪 Germany"},
		{"NL", "🇳🇱 Netherlands"},
		{"IR", "🇮🇷 Iran"},
		{"RU", "🇷🇺 Russia"},
	}
	for _, test := range tests {
		if got := urlStageNodeName(test.code); got != test.want {
			t.Errorf("urlStageNodeName(%q) = %q, want %q", test.code, got, test.want)
		}
	}
}

func TestNodeNameUsesExplicitUnknownFallback(t *testing.T) {
	got := renderNodeName("{flag} {country} {download}↓{upload}↑", "", 0, 0)
	want := "🌐 Unknown 0↓0↑"
	if got != want {
		t.Fatalf("renderNodeName() = %q, want %q", got, want)
	}
}

func TestMeasuredNodeNamesRefreshAllAndURLLists(t *testing.T) {
	dir := t.TempDir()
	parsed := []sub.Node{{Key: "same", Protocol: "vless", Name: "source label", URI: "vless://node#source"}}
	urlNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "🇩🇪 Germany", URI: "vless://node#country", URLPassed: true}}
	measured := []sub.Node{{Key: "same", Protocol: "vless", Name: "🇩🇪 Germany 72↓32↑", URI: "vless://node#🇩🇪 Germany 72↓32↑", URLPassed: true, DownloadMbps: 72, UploadMbps: 32}}

	applyNodeUpdatesByIdentity(parsed, measured)
	applyNodeUpdatesByIdentity(urlNodes, measured)
	if err := writeSubscription(filepath.Join(dir, "all.txt"), parsed); err != nil {
		t.Fatal(err)
	}
	if err := writeStageFiles(dir, "url-tested", urlNodes, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"all.txt", "url-tested.txt", "url-tested-vless.txt"} {
		data := string(mustRead(t, filepath.Join(dir, name)))
		if !strings.Contains(data, "🇩🇪 Germany 72↓32↑") {
			t.Fatalf("%s did not receive the measured name: %q", name, data)
		}
	}
}

func TestURLLiveDetailReportsRenameFailureSeparately(t *testing.T) {
	node := sub.Node{URLPassed: true, RenameError: "GeoIP country lookup failed"}
	got := urlLiveDetail(node)
	if !strings.Contains(got, "URL: PASS") || !strings.Contains(got, "rename: GeoIP country lookup failed") {
		t.Fatalf("unexpected URL live detail: %q", got)
	}
}

func TestRunPreservesPreviousResultsWhenSourceListCannotBeRead(t *testing.T) {
	dir := t.TempDir()
	outDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outDir, 0700); err != nil {
		t.Fatal(err)
	}
	previous := map[string]string{
		"all.txt": "previous successful result\n",
		"url-tested.txt": "previous passing nodes\n",
	}
	for name, content := range previous {
		if err := os.WriteFile(filepath.Join(outDir, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}

	opts := DefaultOptions()
	opts.Sources = filepath.Join(dir, "missing-sources.txt")
	opts.OutDir = outDir
	err := Run(context.Background(), opts, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected missing source list error")
	}
	for name, want := range previous {
		got, readErr := os.ReadFile(filepath.Join(outDir, name))
		if readErr != nil {
			t.Fatalf("previous result %s was removed: %v", name, readErr)
		}
		if string(got) != want {
			t.Fatalf("previous result %s changed: got %q want %q", name, got, want)
		}
	}
}

func TestInitNeverOverwritesExistingFiles(t *testing.T) {
	dir := t.TempDir()
	existing := map[string]string{
		"sources.txt": "https://custom.example/subscription\n",
		"config.json": "{\n  \"custom\": true\n}\n",
	}
	for name, content := range existing {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := Init(dir); err != nil {
		t.Fatal(err)
	}
	for name, want := range existing {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Fatalf("Init overwrote %s: got %q, want %q", name, got, want)
		}
	}
}

