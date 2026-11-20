package app

import (
	"bytes"
	"context"
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
  "network":{"interface":"throne-tun"},
  "sources":{"file":"subs.txt"},
  "protocols":{"enabled":["vless","shadowsocks"]},
  "url_test":{"url":"https://example.com/204","concurrency":6,"timeout":"9s"},
  "speed_test":{"concurrency":3,"timeout":"45s","min_download_mbps":7,"min_upload_mbps":4,"download_bytes":1048576,"upload_bytes":524288,"download_url":"https://download.example/file?size={bytes}","upload_url":"https://upload.example/test"},
  "service_check":{"concurrency":2,"timeout":"20s","lookup_ip":true},
  "final_check":{"enabled":false,"concurrency":1,"timeout":"8s"},
  "output":{"directory":"results","split_by_protocol":true,"subscription_text":false}
}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := LoadOptions(path)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Interface != "throne-tun" || filepath.Base(opts.Sources) != "subs.txt" || filepath.Dir(opts.Sources) != dir || opts.OutDir != "results" {
		t.Fatalf("unexpected paths/network: %+v", opts)
	}
	if opts.URLConcurrency != 6 || opts.SpeedConcurrency != 3 || opts.ServiceConcurrency != 2 {
		t.Fatalf("unexpected concurrency: %+v", opts)
	}
	if opts.URLTimeout != 9*time.Second || opts.SpeedTimeout != 45*time.Second || opts.ServiceTimeout != 20*time.Second {
		t.Fatalf("unexpected timeouts: %+v", opts)
	}
	if len(opts.Protocols) != 2 || opts.Protocols[1] != "ss" {
		t.Fatalf("protocol aliases not normalized: %#v", opts.Protocols)
	}
	if opts.MinDownload != 7 || opts.MinUpload != 4 || !opts.LookupIP || !opts.SplitByProtocol || opts.WriteText {
		t.Fatalf("unexpected stage settings: %+v", opts)
	}
	if opts.DownloadBytes != 1048576 || opts.UploadBytes != 524288 || opts.SpeedDownloadURL != "https://download.example/file?size={bytes}" || opts.SpeedUploadURL != "https://upload.example/test" {
		t.Fatalf("custom speed-test settings were not loaded: %+v", opts)
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
	if err := writeStageFiles(dir, "url-test-finished", nodes, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"url-test-finished.txt", "url-test-finished-ss.txt", "url-test-finished-vless.txt"} {
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
	for _, name := range []string{"all.json", "final.json", "final-vless.json", "report.csv.json", "keep.json"} {
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
	for _, name := range []string{"report.csv.json", "keep.json"} {
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

func TestWorkingProtocolFilesContainOnlyURLPassedNodes(t *testing.T) {
	dir := t.TempDir()
	nodes := []sub.Node{
		{Protocol: "vless", URI: "vless://passed-one"},
		{Protocol: "vless", URI: "vless://passed-two"},
		{Protocol: "ss", URI: "ss://passed"},
	}
	if err := writeWorkingProtocolFiles(dir, nodes); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"vless.txt": "vless://passed-one\nvless://passed-two\n",
		"ss.txt":    "ss://passed\n",
	} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s: got %q, err %v", name, got, err)
		}
	}
	// An empty protocol group must not leave a stale subscription behind.
	if err := writeWorkingProtocolFiles(dir, []sub.Node{{Protocol: "ss", URI: "ss://only"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "vless.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale vless list remains: %v", err)
	}
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
	for _, name := range []string{"config.json", "all.txt", "selected.txt", "url-test-failed.txt", "final-vless.txt", "report.csv", "vless.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := cleanOutputArtifacts(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"all.txt", "selected.txt", "url-test-failed.txt", "final-vless.txt", "report.csv", "vless.txt"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("generated artifact %s remains: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("config was removed: %v", err)
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

func TestCountURLPassesBySourceDeduplicatesPerSourceAndCreditsSharedNodes(t *testing.T) {
	perSource := [][]sub.Node{
		{
			{Key: "node-1"},
			{Key: "node-2"},
			{Key: "node-2"}, // duplicate in one source counts once
			{Key: "node-3"}, // failed URL test
		},
		{
			{Key: "node-2"}, // shared node counts for both subscriptions
			{Key: "node-4"},
		},
	}
	passed := []sub.Node{{Key: "node-1"}, {Key: "node-2"}}
	got := countURLPassesBySource(perSource, passed)
	if len(got) != 2 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("unexpected per-source counts: %#v", got)
	}
}

func TestPrintSubscriptionPassSummarySortsDescendingAndKeepsStableTies(t *testing.T) {
	sources := []loadedSource{
		{Reference: "https://source-a.example/sub"},
		{Reference: "https://source-b.example/sub"},
		{Reference: "https://source-c.example/sub"},
	}
	counts := []int{2, 5, 5}
	var output bytes.Buffer
	printSubscriptionPassSummary(&output, sources, counts)
	got := output.String()
	wantOrder := []string{
		"1. 5 URL-passed configs | https://source-b.example/sub",
		"2. 5 URL-passed configs | https://source-c.example/sub",
		"3. 2 URL-passed configs | https://source-a.example/sub",
	}
	last := -1
	for _, item := range wantOrder {
		index := strings.Index(got, item)
		if index < 0 || index <= last {
			t.Fatalf("output is not sorted/stable; missing or misplaced %q in:\n%s", item, got)
		}
		last = index
	}
}

func TestLiveStageRendersOneReplaceableStatusLine(t *testing.T) {
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

func TestRemoteUploadConfigAndDefaults(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	config := `{
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
	if !opts.RemoteUpload.Enabled || opts.RemoteUpload.Method != "sftp" || opts.RemoteUpload.Port != 2222 || opts.RemoteUpload.Auth != "private_key" {
		t.Fatalf("unexpected remote upload options: %+v", opts.RemoteUpload)
	}
	if opts.RemoteUpload.PrivateKey == "~/.ssh/id_ed25519" {
		t.Fatal("private key home shortcut was not expanded")
	}
}
