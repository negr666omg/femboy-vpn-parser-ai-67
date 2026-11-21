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
  "network":{"interface":"throne-tun","isolate_source_downloads":true},
  "sources":{"file":"subs.txt","timeout":"90s"},
  "protocols":{"enabled":["vless","shadowsocks"]},
  "url_test":{"url":"https://example.com/204","concurrency":6,"timeout":"9s"},
  "speed_test":{"provider":"ookla","concurrency":3,"timeout":"45s","first_byte_timeout":"6s","max_duration":"17s","sample_interval":"500ms","min_download_mbps":7,"min_upload_mbps":4,"download_bytes":1048576,"upload_bytes":524288,"download_url":"https://download.example/file?size={bytes}","upload_url":"https://upload.example/test"},
  "service_check":{"concurrency":2,"timeout":"20s","lookup_ip":true,"rename_nodes":true,"geoip_url":"https://geo.example/json","name_format":"{flag} {download}↓{upload}↑","speed_decimals":2},
  "final_check":{"enabled":false,"concurrency":1,"timeout":"8s"},
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
	if opts.SourceTimeout != 90*time.Second || opts.URLTimeout != 9*time.Second || opts.SpeedTimeout != 45*time.Second || opts.ServiceTimeout != 20*time.Second || opts.FirstByteTimeout != 6*time.Second || opts.MaxSpeedDuration != 17*time.Second || opts.SpeedSampleInterval != 500*time.Millisecond {
		t.Fatalf("unexpected timeouts: %+v", opts)
	}
	if len(opts.Protocols) != 2 || opts.Protocols[1] != "ss" {
		t.Fatalf("protocol aliases not normalized: %#v", opts.Protocols)
	}
	if opts.MinDownload != 7 || opts.MinUpload != 4 || !opts.LookupIP || !opts.RenameNodes || opts.GeoIPURL != "https://geo.example/json" || opts.NameFormat != "{flag} {download}↓{upload}↑" || opts.SpeedDecimals != 2 || !opts.SplitByProtocol || opts.WriteText || opts.GeminiFile {
		t.Fatalf("unexpected stage settings: %+v", opts)
	}
	if opts.SpeedProvider != "ookla" { t.Fatalf("speed provider = %q, want ookla", opts.SpeedProvider) }
	if opts.DownloadBytes != 1048576 || opts.UploadBytes != 524288 || opts.SpeedDownloadURL != "https://download.example/file?size={bytes}" || opts.SpeedUploadURL != "https://upload.example/test" {
		t.Fatalf("custom speed-test settings were not loaded: %+v", opts)
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
		{"url-test-finished", urlNodes},
		{"speed-test-finished", speedNodes},
		{"gemini", geminiNodes},
	} {
		if err := writeStageFiles(dir, item.base, item.nodes, true); err != nil {
			t.Fatal(err)
		}
	}
	for name, want := range map[string]string{
		"url-test-finished-vless.txt": "vless://url-one\n",
		"url-test-finished-ss.txt": "ss://url-two\n",
		"speed-test-finished.txt": "vless://speed-one\n",
		"speed-test-finished-vless.txt": "vless://speed-one\n",
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
	if err := writeStageFiles(dir, "url-test-finished", []sub.Node{{Protocol: "ss", URI: "ss://only"}}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "url-test-finished-vless.txt")); !os.IsNotExist(err) {
		t.Fatal("stale URL protocol list remains")
	}
}

func TestGeminiFileToggleSeparatesGeminiSubscriptions(t *testing.T) {
	dir := t.TempDir()
	parsed := []sub.Node{{Key: "same", Protocol: "vless", Name: "old", URI: "vless://node#old"}}
	urlNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "old", URI: "vless://node#old", URLPassed: true}}
	speedNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "🇫🇮 80↓50↑", URI: "vless://node#fast", URLPassed: true, SpeedPassed: true}}
	geminiNodes := []sub.Node{{Key: "same", Protocol: "vless", Name: "🇫🇮 80↓50↑", URI: "vless://node#fast", URLPassed: true, SpeedPassed: true, GeminiStatus: "available"}}
	if err := writeStageFiles(dir, "url-test-finished", urlNodes, true); err != nil {
		t.Fatal(err)
	}
	if err := writeStageFiles(dir, "speed-test-finished", speedNodes, true); err != nil {
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
	if !strings.Contains(geminiNodes[0].URI, "#fast") {
		t.Fatal("separate Gemini mode unexpectedly renamed node")
	}
	if strings.Contains(string(mustRead(t, filepath.Join(dir, "url-test-finished.txt"))), "Gemini") {
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
	for _, name := range []string{"all.txt", "url-test-finished.txt", "url-test-finished-vless.txt", "speed-test-finished.txt", "speed-test-finished-vless.txt"} {
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
	for _, name := range []string{"config.json", "all.txt", "selected.txt", "url-test-failed.txt", "final-vless.txt", "report.csv", "vless.txt", "gemini.txt", "gemini-available.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := cleanOutputArtifacts(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"all.txt", "selected.txt", "url-test-failed.txt", "final-vless.txt", "report.csv", "vless.txt", "gemini.txt", "gemini-available.txt"} {
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
	if got := sourceProgressTitle(progress.Reference); got != "subscription.example" {
		t.Fatalf("source title leaked URL details or is unclear: %q", got)
	}
	line := formatSourceProgressLine(progress, 80)
	if strings.Contains(line, "secret") || strings.Contains(line, "token") || strings.Contains(line, "/private") {
		t.Fatalf("source progress leaked credentials or URL tokens: %q", line)
	}
	if got := displayWidth(line); got > 79 {
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
	cases := []struct{ value float64; decimals int; want string }{
		{70, 0, "70"}, {70.4, 2, "70"}, {70.5, 1, "71"}, {0.4, 2, "0"}, {0.5, 1, "1"}, {0.55, 2, "1"},
	}
	for _, tc := range cases {
		if got := formatMbps(tc.value, tc.decimals); got != tc.want { t.Errorf("formatMbps(%v,%d)=%q want %q", tc.value, tc.decimals, got, tc.want) }
	}
}


func TestInvalidConfigOutputIsWrittenAndCleanedBetweenRuns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.txt")
	lines := []string{
		"vless://missing-port",
		"unsupported://example.test:443",
	}

	if err := writeInvalidLines(path, lines); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), strings.Join(lines, "\n")+"\n"; got != want {
		t.Fatalf("invalid.txt=%q, want %q", got, want)
	}
	if err := cleanOutputArtifacts(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("invalid.txt should be removed before the next run, stat err=%v", err)
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
