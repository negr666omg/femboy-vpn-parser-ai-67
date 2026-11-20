package app

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unsafe"

	"github.com/negr666omg/subwatch/internal/check"
	"github.com/negr666omg/subwatch/internal/core"
	"github.com/negr666omg/subwatch/internal/sub"
)

func Init(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	for name, body := range map[string]string{
		"sources.txt": "# One subscription URL, local file path, or single share link per line.\n# https://example.net/subscription\n# ./my-subscription.txt\n",
		"config.json": ExampleConfig(),
	} {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			return err
		}
	}
	return nil
}

func Interfaces(w io.Writer) error {
	items, err := net.Interfaces()
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "NAME\tSTATE\tMTU\tADDRESSES")
	for _, it := range items {
		addrs, _ := it.Addrs()
		values := make([]string, 0, len(addrs))
		for _, a := range addrs {
			values = append(values, a.String())
		}
		state := "down"
		if it.Flags&net.FlagUp != 0 {
			state = "up"
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", it.Name, state, it.MTU, strings.Join(values, ", "))
	}
	return nil
}

type loadedSource struct {
	Reference string
	Body      []byte
	Line      int
	LoadError error
}

type sourcePassCount struct {
	Reference string
	Count     int
}

func Run(ctx context.Context, opts Options, stdout, stderr io.Writer) error {
	if opts.URLConcurrency < 1 || opts.URLConcurrency > 64 {
		return fmt.Errorf("url_test.concurrency must be between 1 and 64")
	}
	if opts.SpeedConcurrency < 1 || opts.SpeedConcurrency > 64 {
		return fmt.Errorf("speed_test.concurrency must be between 1 and 64")
	}
	if opts.ServiceConcurrency < 1 || opts.ServiceConcurrency > 64 {
		return fmt.Errorf("service_check.concurrency must be between 1 and 64")
	}
	if opts.URLTimeout <= 0 || opts.SpeedTimeout <= 0 || opts.ServiceTimeout <= 0 {
		return fmt.Errorf("all stage timeouts must be positive")
	}
	if opts.MinDownload < 0 || opts.MinUpload < 0 {
		return fmt.Errorf("minimum speeds cannot be negative")
	}
	if opts.Interface == "" {
		opts.Interface = "auto"
	}
	if opts.Interface != "auto" {
		if _, err := net.InterfaceByName(opts.Interface); err != nil {
			return fmt.Errorf("network interface %q not found (run 'sw interfaces')", opts.Interface)
		}
	}
	if opts.Sources == "" {
		opts.Sources = "sources.txt"
	}
	if opts.OutDir == "" {
		opts.OutDir = "out"
	}
	if opts.URL == "" {
		opts.URL = "https://www.gstatic.com/generate_204"
	}
	if opts.SpeedDownloadURL == "" {
		opts.SpeedDownloadURL = "https://speed.cloudflare.com/__down?bytes={bytes}"
	}
	if opts.SpeedUploadURL == "" {
		opts.SpeedUploadURL = "https://speed.cloudflare.com/__up"
	}
	if opts.DownloadBytes == 0 {
		opts.DownloadBytes = 8 << 20
	}
	if opts.UploadBytes == 0 {
		opts.UploadBytes = 4 << 20
	}
	if opts.DownloadBytes < 64*1024 || opts.UploadBytes < 64*1024 {
		return fmt.Errorf("download/upload test payload must be at least 64 KiB")
	}
	if opts.DownloadBytes > 100<<20 || opts.UploadBytes > 100<<20 {
		return fmt.Errorf("download/upload test payload cannot exceed 100 MiB")
	}
	if len(opts.Protocols) == 0 {
		opts.Protocols = DefaultOptions().Protocols
	}
	opts.Protocols = normalizeProtocols(opts.Protocols)
	if err := os.MkdirAll(opts.OutDir, 0700); err != nil {
		return err
	}
	if err := os.Chmod(opts.OutDir, 0700); err != nil {
		return err
	}
	// Keep the output directory focused on the five user-facing result groups.
	// Intermediate selections, failed lists, CSV diagnostics and obsolete files
	// are deliberately not exported.
	if err := cleanOutputArtifacts(opts.OutDir); err != nil {
		return fmt.Errorf("clean output directory: %w", err)
	}

	fmt.Fprintf(stdout, "[1/4] PARSE: reading %s\n", opts.Sources)
	sources, err := loadSources(ctx, opts.Sources)
	if err != nil {
		return err
	}
	loadedCount, skippedCount := 0, 0
	for _, source := range sources {
		if source.LoadError != nil {
			skippedCount++
		} else {
			loadedCount++
		}
	}
	fmt.Fprintf(stdout, "Sources: %d listed | %d loaded | %d skipped\n", len(sources), loadedCount, skippedCount)
	sourceBodies := make([][]byte, len(sources))
	for i := range sources {
		sourceBodies[i] = sources[i].Body
	}
	metadata := collectMetadata(sourceBodies)

	var parsed []sub.Node
	// Keep the per-source node lists as well as the global list. Nodes are
	// tested once globally, then credited to every source that contained them.
	perSourceNodes := make([][]sub.Node, len(sources))
	invalid, sourceErrors := 0, 0
	for i, source := range sources {
		if source.LoadError != nil {
			sourceErrors++
			lineNo := source.Line
			if lineNo < 1 {
				lineNo = i + 1
			}
			fmt.Fprintf(stderr, "source %d (%s): skipped: %v\n", lineNo, source.Reference, source.LoadError)
			continue
		}
		nodes, bad, parseErr := sub.ParseDocument(source.Body)
		invalid += bad
		if parseErr != nil {
			sourceErrors++
			lineNo := source.Line
			if lineNo < 1 {
				lineNo = i + 1
			}
			fmt.Fprintf(stderr, "source %d (%s): %v\n", lineNo, source.Reference, parseErr)
			continue
		}
		perSourceNodes[i] = nodes
		parsed = append(parsed, nodes...)
	}
	if len(parsed) == 0 {
		return fmt.Errorf("no supported proxy nodes found (invalid lines: %d; source failures: %d; check source URLs and retry)", invalid, sourceErrors)
	}
	if err := writeSubscription(filepath.Join(opts.OutDir, "all.txt"), parsed); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Parsed: %d | invalid: %d | source errors: %d | metadata lines: %d\n", len(parsed), invalid, sourceErrors, len(metadata))

	// Keep pipeline state in memory. Exported artifacts are plain URI lists,
	// not serialized Node objects, and are never read back as internal state.
	unique, duplicates := deduplicate(parsed)
	selected := filterProtocols(unique, opts.Protocols)
	if len(selected) == 0 {
		return fmt.Errorf("no nodes match enabled protocols: %s", strings.Join(opts.Protocols, ", "))
	}
	fmt.Fprintf(stdout, "Deduplicated: %d unique | %d duplicates | enabled protocols: %s | nodes to test: %d\n", len(unique), duplicates, strings.Join(opts.Protocols, ","), len(selected))
	fmt.Fprintf(stdout, "Interface: %s | URL concurrency: %d | speed concurrency: %d | service concurrency: %d\n", opts.Interface, opts.URLConcurrency, opts.SpeedConcurrency, opts.ServiceConcurrency)
	fmt.Fprintf(stdout, "Speed payload: %.2f/%.2f MiB | minimum DL/UL: %.2f/%.2f Mbps | download URL: %s | upload URL: %s\n", float64(opts.DownloadBytes)/(1<<20), float64(opts.UploadBytes)/(1<<20), opts.MinDownload, opts.MinUpload, opts.SpeedDownloadURL, opts.SpeedUploadURL)

	fmt.Fprintln(stdout, "[prepare] Checking sing-box; first run may download the verified core...")
	corePath, err := core.FindOrDownloadProgress(opts.CorePath, stdout)
	if err != nil {
		return fmt.Errorf("sing-box: %w", err)
	}
	opts.CorePath = corePath
	fmt.Fprintf(stdout, "[prepare] sing-box ready: %s\n", corePath)

	// Stage 1: every selected unique node gets the URL test. Nothing is speed-tested here.
	fmt.Fprintf(stdout, "\n[2/4] URL TEST: %d nodes, target %s, timeout %s\n", len(selected), opts.URL, opts.URLTimeout)
	var urlPassedLive []sub.Node
	var urlFilesMu sync.Mutex
	urlResults, stageErr := runStageWithLiveCallback(ctx, "URL TEST", selected, opts.URLConcurrency, stdout, func(n *sub.Node) bool { return n.URLPassed }, func(stageCtx context.Context, n *sub.Node, live func(sub.Node, string)) {
		live(*n, "checking URL…")
		passed, latency, testErr := runURLTest(stageCtx, *n, opts)
		n.URLPassed, n.URLLatencyMS = passed, latency
		if testErr != nil {
			n.URLError = testErr.Error()
		} else if !passed {
			n.URLError = "URL test returned non-success status"
		}
		live(*n, urlLiveDetail(*n))
	}, func(completed sub.Node) error {
		if !completed.URLPassed {
			return nil
		}
		urlFilesMu.Lock()
		defer urlFilesMu.Unlock()
		urlPassedLive = append(urlPassedLive, completed)
		if err := writeSubscription(filepath.Join(opts.OutDir, "url-test-finished.txt"), urlPassedLive); err != nil {
			return err
		}
		return writeWorkingProtocolFiles(opts.OutDir, urlPassedLive)
	})
	if stageErr != nil {
		return stageErr
	}
	urlPassed := filterNodes(urlResults, func(n sub.Node) bool { return n.URLPassed })
	sourcePassCounts := countURLPassesBySource(perSourceNodes, urlPassed)
	fmt.Fprintf(stdout, "URL test complete: %d passed / %d failed. Protocol subscriptions were updated during testing.\n", len(urlPassed), len(urlResults)-len(urlPassed))

	// Stage 2 begins only after every URL test has completed (global barrier).
	// urlPassed remains the authoritative in-memory result set.
	fmt.Fprintf(stdout, "\n[3/4] SPEED TEST: %d URL-passed nodes, timeout %s, minimum DL/UL %.2f/%.2f Mbps\n", len(urlPassed), opts.SpeedTimeout, opts.MinDownload, opts.MinUpload)
	speedResults, _ := runStageWithLiveCallback(ctx, "SPEED TEST", urlPassed, opts.SpeedConcurrency, stdout, func(n *sub.Node) bool { return n.SpeedPassed }, func(stageCtx context.Context, n *sub.Node, live func(sub.Node, string)) {
		live(*n, speedLiveDetail(*n, "starting speed test…"))
		down, up, testErr := runSpeedTestWithProgress(stageCtx, *n, opts, func(progress check.SpeedProgress) {
			live(*n, speedProgressDetail(*n, progress))
		})
		n.DownloadMbps, n.UploadMbps = down, up
		if testErr != nil {
			n.SpeedError = testErr.Error()
			live(*n, speedLiveDetail(*n, "FAILED: "+shortLiveError(testErr.Error())))
			return
		}
		if reason := speedThresholdError(down, up, opts.MinDownload, opts.MinUpload); reason != "" {
			n.SpeedError = reason
			live(*n, speedLiveDetail(*n, "FAILED: "+shortLiveError(reason)))
			return
		}
		n.SpeedPassed = true
		if opts.RenameNodes {
			geoErr := withNodeClient(stageCtx, *n, opts, opts.ServiceTimeout, func(ctx context.Context, client *http.Client) error {
				country := lookupCountry(ctx, client, opts.GeoIPURL)
				flag := countryFlag(country)
				if flag == "" { return fmt.Errorf("GeoIP country lookup failed") }
				n.Country = country
				name := strings.NewReplacer("{flag}", flag, "{download}", formatMbps(down, opts.SpeedDecimals), "{upload}", formatMbps(up, opts.SpeedDecimals)).Replace(opts.NameFormat)
				if name == opts.NameFormat { return fmt.Errorf("service_check.name_format has no supported placeholders") }
				sub.RenameNode(n, name)
				return nil
			})
			if geoErr != nil { n.SpeedError = "rename: " + geoErr.Error() }
		}
		live(*n, speedLiveDetail(*n, "PASS"))
	}, nil)
	speedPassed := filterNodes(speedResults, func(n sub.Node) bool { return n.SpeedPassed })
	speedFailed := filterNodes(speedResults, func(n sub.Node) bool { return !n.SpeedPassed })
	if err := writeSubscription(filepath.Join(opts.OutDir, "speedtest-finished.txt"), speedPassed); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Speed test complete: %d passed / %d failed.\n", len(speedPassed), len(speedFailed))

	// Stage 3 starts only after every speed test has completed. No service check
	// is run on a node that failed either speed threshold.
	fmt.Fprintf(stdout, "\n[4/4] SERVICE CHECK: %d speed-passed nodes, Gemini must be fully available\n", len(speedPassed))
	serviceResults, _ := runStageWithLiveCallback(ctx, "SERVICE CHECK", speedPassed, opts.ServiceConcurrency, stdout, func(n *sub.Node) bool { return n.GeminiStatus == "available" }, func(stageCtx context.Context, n *sub.Node, live func(sub.Node, string)) {
		live(*n, speedLiveDetail(*n, "Gemini: checking…"))
		status, note, region, ipInfo, testErr := runServiceCheck(stageCtx, *n, opts)
		n.GeminiStatus, n.GeminiNote, n.GeminiRegion = status, note, region
		if testErr != nil {
			n.ServiceError = testErr.Error()
			n.GeminiStatus = "failed"
		}
		if opts.LookupIP && testErr == nil {
			check.ApplyIP(n, ipInfo)
		}
		detail := speedLiveDetail(*n, "Gemini: "+n.GeminiStatus)
		if n.GeminiRegion != "" {
			detail += " | region: " + n.GeminiRegion
		}
		if n.GeminiNote != "" {
			detail += " | " + n.GeminiNote
		}
		live(*n, detail)
	}, nil)
	finalCandidates := filterNodes(serviceResults, func(n sub.Node) bool { return n.GeminiStatus == "available" })
	// Each node is tested exactly once per enabled stage: URL, speed, then Gemini.
	// There is deliberately no final recheck stage; it duplicated URL and Gemini requests.
	finalNodes := finalCandidates
	if err := writeSubscription(filepath.Join(opts.OutDir, "gemini-available.txt"), finalNodes); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "\nFinished. Parsed: %d | unique: %d | URL passed: %d | speed passed: %d | Gemini available: %d\n", len(parsed), len(unique), len(urlPassed), len(speedPassed), len(finalNodes))
	printSubscriptionPassSummary(stdout, sources, sourcePassCounts)
	fmt.Fprintf(stdout, "Output directory: %s\n", opts.OutDir)
	fmt.Fprintln(stdout, "Output: all.txt, url-test-finished.txt, speedtest-finished.txt, gemini-available.txt, and protocol-only working lists (all files contain URI lines)")
	if opts.RemoteUpload.Enabled {
		if uploadErr := uploadOutput(ctx, opts.RemoteUpload, opts.OutDir, stdout, stderr); uploadErr != nil {
			fmt.Fprintf(stderr, "[UPLOAD] WARNING: %v\n", uploadErr)
		}
	}
	return ctx.Err()
}

const sourceDownloadTimeout = 30 * time.Second

func loadSources(ctx context.Context, path string) ([]loadedSource, error) {
	return loadSourcesWithTimeout(ctx, path, sourceDownloadTimeout)
}

// loadSourcesWithTimeout treats a failing subscription as an isolated source
// failure. A timeout or HTTP error skips that entry and continues loading the
// remaining sources. Only cancellation of the whole run aborts the operation.
func loadSourcesWithTimeout(ctx context.Context, path string, timeout time.Duration) ([]loadedSource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(data) > 2<<20 {
		return nil, fmt.Errorf("%s exceeds 2 MiB", path)
	}
	var sources []loadedSource
	client := &http.Client{Timeout: timeout}
	for lineNo, line := range strings.Split(string(data), "\n") {
		if err := ctx.Err(); err != nil {
			return sources, err
		}
		entry := strings.TrimSpace(strings.TrimPrefix(line, "\ufeff"))
		if entry == "" || strings.HasPrefix(entry, "#") {
			continue
		}
		source := loadedSource{Reference: entry, Line: lineNo + 1}
		if strings.HasPrefix(entry, "http://") || strings.HasPrefix(entry, "https://") {
			u, parseErr := url.Parse(entry)
			if parseErr != nil || u.Host == "" {
				source.LoadError = fmt.Errorf("invalid subscription URL")
				sources = append(sources, source)
				continue
			}
			req, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, entry, nil)
			if requestErr != nil {
				source.LoadError = fmt.Errorf("create request: %w", requestErr)
				sources = append(sources, source)
				continue
			}
			req.Header.Set("User-Agent", "SubWatch/0.3.7")
			resp, requestErr := client.Do(req)
			if requestErr != nil {
				if ctx.Err() != nil {
					return sources, ctx.Err()
				}
				source.LoadError = fmt.Errorf("download failed: %w", requestErr)
				sources = append(sources, source)
				continue
			}
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
			closeErr := resp.Body.Close()
			if readErr != nil {
				if ctx.Err() != nil {
					return sources, ctx.Err()
				}
				source.LoadError = fmt.Errorf("read response: %w", readErr)
			} else if len(body) > 8<<20 {
				source.LoadError = fmt.Errorf("subscription exceeds 8 MiB")
			} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				source.LoadError = fmt.Errorf("server returned HTTP %d", resp.StatusCode)
			} else if closeErr != nil {
				source.LoadError = fmt.Errorf("close response: %w", closeErr)
			} else {
				source.Body = body
			}
		} else if strings.Contains(entry, "://") {
			source.Body = []byte(entry)
		} else {
			local, readErr := os.ReadFile(entry)
			if readErr != nil {
				source.LoadError = fmt.Errorf("read local source: %w", readErr)
			} else {
				source.Body = local
			}
		}
		sources = append(sources, source)
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("%s contains no sources", path)
	}
	return sources, nil
}

func collectMetadata(sources [][]byte) []string {
	seen := map[string]bool{}
	var out []string
	for _, source := range sources {
		for _, line := range strings.Split(string(source), "\n") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "\ufeff"))
			if line == "" {
				continue
			}
			keep := false
			for _, prefix := range []string{"#profile-", "#announce", "#support-url"} {
				if strings.HasPrefix(strings.ToLower(line), prefix) {
					keep = true
					break
				}
			}
			if keep && !seen[line] {
				seen[line] = true
				out = append(out, line)
			}
		}
	}
	return out
}

func nodeDedupKey(n sub.Node) string {
	if n.Key != "" {
		return n.Key
	}
	b, _ := json.Marshal(n.Outbound)
	return string(b)
}

func countURLPassesBySource(perSourceNodes [][]sub.Node, urlPassed []sub.Node) []int {
	passedKeys := make(map[string]struct{}, len(urlPassed))
	for _, node := range urlPassed {
		passedKeys[nodeDedupKey(node)] = struct{}{}
	}
	counts := make([]int, len(perSourceNodes))
	for sourceIndex, nodes := range perSourceNodes {
		seen := make(map[string]struct{}, len(nodes))
		for _, node := range nodes {
			key := nodeDedupKey(node)
			if _, exists := passedKeys[key]; !exists {
				continue
			}
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			counts[sourceIndex]++
		}
	}
	return counts
}

func printSubscriptionPassSummary(w io.Writer, sources []loadedSource, counts []int) {
	items := make([]sourcePassCount, len(sources))
	for i, source := range sources {
		count := 0
		if i < len(counts) {
			count = counts[i]
		}
		items[i] = sourcePassCount{Reference: source.Reference, Count: count}
	}
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].Count > items[j].Count
	})

	fmt.Fprintln(w, "\nSubscription URL-test results (highest passing count first):")
	for rank, item := range items {
		word := "configs"
		if item.Count == 1 {
			word = "config"
		}
		fmt.Fprintf(w, "%2d. %d URL-passed %s | %s\n", rank+1, item.Count, word, item.Reference)
	}
	if len(items) == 0 {
		fmt.Fprintln(w, "  No subscription sources")
	}
}

func deduplicate(nodes []sub.Node) ([]sub.Node, int) {
	seen := map[string]bool{}
	out := make([]sub.Node, 0, len(nodes))
	duplicates := 0
	for _, n := range nodes {
		key := nodeDedupKey(n)
		if seen[key] {
			duplicates++
			continue
		}
		seen[key] = true
		out = append(out, n)
	}
	return out, duplicates
}

func filterProtocols(nodes []sub.Node, enabled []string) []sub.Node {
	allowed := map[string]bool{}
	for _, p := range enabled {
		allowed[p] = true
	}
	return filterNodes(nodes, func(n sub.Node) bool { return allowed[protocolOf(n)] })
}

func protocolOf(n sub.Node) string {
	if n.Protocol != "" {
		return n.Protocol
	}
	if p, ok := n.Outbound["type"].(string); ok {
		switch strings.ToLower(p) {
		case "shadowsocks":
			return "ss"
		case "hy2":
			return "hysteria2"
		case "socks5":
			return "socks"
		default:
			return strings.ToLower(p)
		}
	}
	return "unknown"
}

func filterNodes(nodes []sub.Node, keep func(sub.Node) bool) []sub.Node {
	out := make([]sub.Node, 0, len(nodes))
	for _, n := range nodes {
		if keep(n) {
			out = append(out, n)
		}
	}
	return out
}

func runStage(ctx context.Context, label string, nodes []sub.Node, concurrency int, stdout io.Writer, passed func(*sub.Node) bool, test func(context.Context, *sub.Node)) []sub.Node {
	results, _ := runStageWithCallback(ctx, label, nodes, concurrency, stdout, passed, test, nil)
	return results
}

func runStageWithCallback(ctx context.Context, label string, nodes []sub.Node, concurrency int, stdout io.Writer, passed func(*sub.Node) bool, test func(context.Context, *sub.Node), onComplete func(sub.Node) error) ([]sub.Node, error) {
	return runStageWithLiveCallback(ctx, label, nodes, concurrency, stdout, passed, func(stageCtx context.Context, node *sub.Node, _ func(sub.Node, string)) {
		test(stageCtx, node)
	}, onComplete)
}

// runStageWithLiveCallback maintains exactly one mutable terminal status line.
// Concurrent workers may test multiple nodes, but only the most recently
// started node owns the visible line; older workers cannot overwrite it later.
func runStageWithLiveCallback(ctx context.Context, label string, nodes []sub.Node, concurrency int, stdout io.Writer, passed func(*sub.Node) bool, test func(context.Context, *sub.Node, func(sub.Node, string)), onComplete func(sub.Node) error) ([]sub.Node, error) {
	results := make([]sub.Node, len(nodes))
	if len(nodes) == 0 {
		fmt.Fprintf(stdout, "[%s] 0/0 (nothing to test)\n", label)
		return results, nil
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(nodes) {
		concurrency = len(nodes)
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	var progressMu sync.Mutex
	var callbackErr error
	done, successes := 0, 0
	var nextTicket, visibleTicket uint64
	visibleNode := sub.Node{}
	visibleDetail := "waiting…"

	render := func() {
		line := formatLiveStatusLine(label, done, len(nodes), successes, visibleNode, visibleDetail, liveTerminalWidth(stdout))
		fmt.Fprintf(stdout, "\r\x1b[2K%s", line)
	}

	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				results[i] = nodes[i]

				progressMu.Lock()
				nextTicket++
				ticket := nextTicket
				visibleTicket = ticket
				visibleNode = results[i]
				visibleDetail = "starting…"
				render()
				progressMu.Unlock()

				emit := func(node sub.Node, detail string) {
					progressMu.Lock()
					if ticket == visibleTicket {
						visibleNode = node
						visibleDetail = cleanLiveText(detail)
						render()
					}
					progressMu.Unlock()
				}

				if ctx.Err() != nil {
					setStageError(&results[i], label, ctx.Err().Error())
				} else {
					test(ctx, &results[i], emit)
				}

				progressMu.Lock()
				if onComplete != nil && callbackErr == nil {
					if err := onComplete(results[i]); err != nil {
						callbackErr = err
					}
				}
				done++
				if passed(&results[i]) {
					successes++
				}
				if ticket == visibleTicket {
					visibleNode = results[i]
					visibleDetail = stageResultDetail(label, results[i])
				}
				render()
				progressMu.Unlock()
			}
		}()
	}
	for i := range nodes {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	fmt.Fprintln(stdout)
	return results, callbackErr
}

func liveNodeTitle(n sub.Node) string {
	name := strings.TrimSpace(n.Name)
	if name == "" {
		if parsed, err := url.Parse(n.URI); err == nil && parsed.Host != "" {
			name = parsed.Host
		} else {
			name = "unnamed config"
		}
	}
	name = cleanLiveText(name)
	if len([]rune(name)) > 64 {
		name = string([]rune(name)[:61]) + "…"
	}
	protocol := protocolOf(n)
	if protocol == "" {
		return name
	}
	return fmt.Sprintf("%s (%s)", name, protocol)
}

func cleanLiveText(value string) string {
	value = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r < 32 || r == 127 {
			return ' '
		}
		return r
	}, value)
	return strings.Join(strings.Fields(value), " ")
}

func urlLiveDetail(n sub.Node) string {
	if n.URLPassed {
		return "URL: PASS"
	}
	if n.URLError != "" {
		return "URL: FAIL (" + shortLiveError(n.URLError) + ")"
	}
	return "URL: FAIL"
}

func speedLiveDetail(n sub.Node, state string) string {
	return fmt.Sprintf("download: %.2f Mbps | upload: %.2f Mbps | %s", n.DownloadMbps, n.UploadMbps, cleanLiveText(state))
}

func speedProgressDetail(n sub.Node, progress check.SpeedProgress) string {
	state := progress.Phase
	if progress.Phase == "download" {
		state = fmt.Sprintf("downloading %d/%d bytes", progress.CompletedBytes, progress.TotalBytes)
	} else if progress.Phase == "upload" {
		state = fmt.Sprintf("uploading %d/%d bytes", progress.CompletedBytes, progress.TotalBytes)
	}
	return fmt.Sprintf("download: %.2f Mbps | upload: %.2f Mbps | %s", progress.DownloadMbps, progress.UploadMbps, state)
}

// formatLiveStatusLine keeps ping as a fixed right-hand field. The progress
// counters remain first; the config name and stage details share the remaining
// width and are shortened as needed. One terminal column is reserved to avoid
// accidental auto-wrap at the right margin.
func formatLiveStatusLine(label string, done, total, passed int, node sub.Node, detail string, terminalWidth int) string {
	head := fmt.Sprintf("[%s] %d/%d | passed: %d | failed: %d", label, done, total, passed, done-passed)
	identity := liveNodeTitle(node)
	detail = cleanLiveText(detail)
	ping := livePingText(node)

	// When stdout is redirected, do not pad output to an assumed width, but
	// always keep ping last so captured logs have the same field order.
	if terminalWidth <= 0 {
		return strings.TrimSpace(head + " | " + identity + " | " + detail + " | " + ping)
	}
	limit := terminalWidth
	if limit > 1 {
		limit--
	}

	pingWidth := displayWidth(ping)
	prefixBudget := limit - pingWidth - 2 // keep at least two spaces before ping
	if prefixBudget < 1 {
		return truncateDisplay(ping, limit)
	}

	fixedWidth := displayWidth(head)
	if fixedWidth >= prefixBudget {
		return rightAlignPing(truncateDisplay(head, prefixBudget), ping, limit)
	}

	remaining := prefixBudget - fixedWidth - 6 // room for " | " twice
	if remaining <= 0 {
		return rightAlignPing(head, ping, limit)
	}

	identityWidth := displayWidth(identity)
	detailWidth := displayWidth(detail)
	// Split the flexible area roughly evenly. Any unused space from a short
	// name/detail is offered to the other field before truncation.
	identityBudget := remaining / 2
	detailBudget := remaining - identityBudget
	if identityWidth < identityBudget {
		detailBudget += identityBudget - identityWidth
	}
	if detailWidth < detailBudget {
		identityBudget += detailBudget - detailWidth
	}
	identityPart := truncateDisplay(identity, identityBudget)
	detailPart := truncateDisplay(detail, detailBudget)

	prefix := head
	if identityPart != "" {
		prefix += " | " + identityPart
	}
	if detailPart != "" {
		prefix += " | " + detailPart
	}
	return rightAlignPing(prefix, ping, limit)
}

func rightAlignPing(prefix, ping string, limit int) string {
	pingWidth := displayWidth(ping)
	maxPrefix := limit - pingWidth - 2
	if maxPrefix < 0 {
		return truncateDisplay(ping, limit)
	}
	prefix = truncateDisplay(prefix, maxPrefix)
	padding := limit - displayWidth(prefix) - pingWidth
	if padding < 2 {
		padding = 2
	}
	return prefix + strings.Repeat(" ", padding) + ping
}

func livePingText(n sub.Node) string {
	// URLLatencyMS is the elapsed time of the URL request. It is a usable ping
	// only when the URL request actually succeeded; failed attempts show an
	// explicit unavailable value instead of presenting timeout duration as ping.
	if !n.URLPassed {
		return "ping: — ms"
	}
	return fmt.Sprintf("ping: %d ms", n.URLLatencyMS)
}

func shortLiveError(message string) string {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "connection reset"):
		return "connection reset"
	case strings.Contains(lower, "connection refused"):
		return "connection refused"
	case strings.Contains(lower, "context deadline exceeded"), strings.Contains(lower, "i/o timeout"):
		return "timeout"
	case strings.Contains(lower, "no such host"):
		return "DNS error"
	case strings.Contains(lower, "eof"):
		return "connection closed"
	default:
		message = cleanLiveText(message)
		return truncateDisplay(message, 48)
	}
}

func liveTerminalWidth(w io.Writer) int {
	if f, ok := w.(*os.File); ok {
		var size struct {
			Row, Col, Xpixel, Ypixel uint16
		}
		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&size)))
		if errno == 0 && size.Col > 0 {
			return int(size.Col)
		}
	}
	// COLUMNS is useful in terminal wrappers that do not expose an *os.File.
	if value, err := strconv.Atoi(os.Getenv("COLUMNS")); err == nil && value > 0 {
		return value
	}
	return 0
}

func displayWidth(value string) int {
	runes := []rune(value)
	width := 0
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if isRegionalIndicator(r) && i+1 < len(runes) && isRegionalIndicator(runes[i+1]) {
			width += 2 // a flag pair occupies two terminal cells, not four
			i++
			continue
		}
		width += runeCellWidth(r)
	}
	return width
}

func runeCellWidth(r rune) int {
	if r == 0 || r == '\n' || r == '\r' || r == '\t' || r == 0x200d ||
		unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r) || unicode.Is(unicode.Cf, r) ||
		(r >= 0x1f3fb && r <= 0x1f3ff) || (r >= 0xfe00 && r <= 0xfe0f) ||
		(r >= 0xe0100 && r <= 0xe01ef) {
		return 0
	}
	if isWideRune(r) {
		return 2
	}
	return 1
}

func isRegionalIndicator(r rune) bool {
	return r >= 0x1f1e6 && r <= 0x1f1ff
}

func isWideRune(r rune) bool {
	return r >= 0x1100 && (r <= 0x115f || r == 0x2329 || r == 0x232a ||
		(r >= 0x2e80 && r <= 0xa4cf) || (r >= 0xac00 && r <= 0xd7a3) ||
		(r >= 0xf900 && r <= 0xfaff) || (r >= 0xfe10 && r <= 0xfe19) ||
		(r >= 0xfe30 && r <= 0xfe6f) || (r >= 0xff00 && r <= 0xff60) ||
		(r >= 0xffe0 && r <= 0xffe6) || (r >= 0x1f300 && r <= 0x1faff) ||
		(r >= 0x20000 && r <= 0x3fffd)) || (r >= 0x2600 && r <= 0x27bf)
}

func truncateDisplay(value string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	if displayWidth(value) <= maxWidth {
		return value
	}
	if maxWidth == 1 {
		return "…"
	}
	limit := maxWidth - 1 // reserve one cell for the ellipsis
	runes := []rune(value)
	var b strings.Builder
	used := 0
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if isRegionalIndicator(r) && i+1 < len(runes) && isRegionalIndicator(runes[i+1]) {
			if used+2 > limit {
				break
			}
			b.WriteRune(r)
			b.WriteRune(runes[i+1])
			used += 2
			i++
			continue
		}
		w := runeCellWidth(r)
		if used+w > limit {
			break
		}
		b.WriteRune(r)
		used += w
	}
	b.WriteRune('…')
	return b.String()
}

func stageResultDetail(label string, n sub.Node) string {
	switch label {
	case "URL TEST":
		return urlLiveDetail(n)
	case "SPEED TEST":
		state := "FAIL"
		if n.SpeedPassed {
			state = "PASS"
		} else if n.SpeedError != "" {
			state += ": " + shortLiveError(n.SpeedError)
		}
		return speedLiveDetail(n, state)
	case "SERVICE CHECK":
		state := "Gemini: " + n.GeminiStatus
		if n.GeminiNote != "" {
			state += " (" + truncateDisplay(cleanLiveText(n.GeminiNote), 28) + ")"
		}
		return speedLiveDetail(n, state)
	default:
		return "completed"
	}
}

func setStageError(n *sub.Node, label, message string) {
	switch label {
	case "URL TEST":
		n.URLError = message
	case "SPEED TEST":
		n.SpeedError = message
	case "SERVICE CHECK":
		n.ServiceError = message
		n.GeminiStatus = "failed"
	}
}

func runURLTest(parent context.Context, n sub.Node, opts Options) (bool, int64, error) {
	var passed bool
	var latency int64
	err := withNodeClient(parent, n, opts, opts.URLTimeout, func(ctx context.Context, client *http.Client) error {
		testCtx, cancel := context.WithTimeout(ctx, opts.URLTimeout)
		defer cancel()
		var testErr error
		passed, latency, testErr = check.URLTest(testCtx, client, opts.URL)
		return testErr
	})
	return passed, latency, err
}

func speedThresholdError(download, upload, minDownload, minUpload float64) string {
	var reasons []string
	if download < minDownload {
		reasons = append(reasons, fmt.Sprintf("download %.2f < %.2f Mbps", download, minDownload))
	}
	if upload < minUpload {
		reasons = append(reasons, fmt.Sprintf("upload %.2f < %.2f Mbps", upload, minUpload))
	}
	return strings.Join(reasons, "; ")
}

func runSpeedTest(parent context.Context, n sub.Node, opts Options) (float64, float64, error) {
	return runSpeedTestWithProgress(parent, n, opts, nil)
}

func runSpeedTestWithProgress(parent context.Context, n sub.Node, opts Options, progress func(check.SpeedProgress)) (float64, float64, error) {
	var down, up float64
	err := withNodeClient(parent, n, opts, opts.SpeedTimeout, func(ctx context.Context, client *http.Client) error {
		testCtx, cancel := context.WithTimeout(ctx, opts.SpeedTimeout)
		defer cancel()
		var testErr error
		down, up, testErr = check.SpeedTestWithProgress(testCtx, client, opts.SpeedDownloadURL, opts.SpeedUploadURL, opts.DownloadBytes, opts.UploadBytes, progress, opts.FirstByteTimeout, opts.MaxSpeedDuration, opts.SpeedSampleInterval)
		return testErr
	})
	return down, up, err
}


func lookupCountry(ctx context.Context, client *http.Client, endpoint string) string {
	if endpoint == "" { endpoint = "https://ipwho.is/" }
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil { return "" }
	resp, err := client.Do(req)
	if err != nil { return "" }
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 { return "" }
	var data map[string]any
	if json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&data) != nil { return "" }
	code, _ := data["country_code"].(string)
	return strings.ToUpper(strings.TrimSpace(code))
}

func countryFlag(code string) string {
	if len(code) != 2 || code[0] < 65 || code[0] > 90 || code[1] < 65 || code[1] > 90 { return "" }
	return string([]rune{rune(0x1F1E6 + code[0] - 65), rune(0x1F1E6 + code[1] - 65)})
}

func formatMbps(value float64, decimals int) string {
	formatted := strconv.FormatFloat(value, 'f', decimals, 64)
	return strings.TrimSuffix(strings.TrimSuffix(formatted, "0"), ".")
}

func runServiceCheck(parent context.Context, n sub.Node, opts Options) (string, string, string, check.IPDetails, error) {
	var status, note, region string
	var info check.IPDetails
	err := withNodeClient(parent, n, opts, opts.ServiceTimeout, func(ctx context.Context, client *http.Client) error {
		serviceCtx, cancel := context.WithTimeout(ctx, opts.ServiceTimeout)
		defer cancel()
		var serviceErr error
		status, note, region, serviceErr = check.GeminiTest(serviceCtx, client)
		if serviceErr != nil {
			return serviceErr
		}
		if opts.LookupIP {
			lookupCtx, lookupCancel := context.WithTimeout(ctx, opts.ServiceTimeout)
			defer lookupCancel()
			info = check.LookupIP(lookupCtx, client)
		}
		return nil
	})
	return status, note, region, info, err
}

func withNodeClient(parent context.Context, n sub.Node, opts Options, timeout time.Duration, fn func(context.Context, *http.Client) error) error {
	startCtx, cancel := context.WithTimeout(parent, timeout+8*time.Second)
	defer cancel()
	inst, err := core.Start(startCtx, opts.CorePath, n.Outbound, opts.Interface)
	if err != nil {
		return fmt.Errorf("start proxy: %w", err)
	}
	defer inst.Close()
	client, err := check.NewClient(inst.ProxyURL, timeout)
	if err != nil {
		return err
	}
	defer check.CloseClient(client)
	return fn(startCtx, client)
}

func writeStageFiles(dir, base string, nodes []sub.Node, split bool) error {
	if err := writeSubscription(filepath.Join(dir, base+".txt"), nodes); err != nil {
		return err
	}
	if !split {
		return clearProtocolSubscriptions(dir, base)
	}
	return writeProtocolSubscriptions(dir, base, nodes)
}

// cleanOutputArtifacts removes only filenames previously generated by SubWatch.
// The output directory is a result directory, so stale stage and split files
// from older versions must not survive a new run.
func cleanOutputArtifacts(dir string) error {
	known := []string{
		"all.txt", "unique.txt", "selected.txt", "url-test-finished.txt", "url-test-failed.txt",
		"speedtest-finished.txt", "speedtest-failed.txt", "tested.txt", "service-check-results.txt",
		"final.txt", "gemini-available.txt", "report.csv",
		"all.json", "unique.json", "selected.json", "url-test-finished.json", "url-test-failed.json",
		"speedtest-finished.json", "speedtest-failed.json", "tested.json", "service-check-results.json", "final.json",
	}
	for _, name := range known {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	// Protocol-only lists from earlier runs are named <protocol>.txt.
	for _, protocol := range []string{"vless", "vmess", "trojan", "ss", "hysteria2", "tuic", "socks", "http", "unknown"} {
		if err := os.Remove(filepath.Join(dir, protocol+".txt")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	patterns := []string{"*-*.txt", "*-*.json"}
	for _, pattern := range patterns {
		paths, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return err
		}
		for _, path := range paths {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

// writeWorkingProtocolFiles exports only URL-test-passed nodes, grouped by protocol.
// It is called after every successful URL test, so subscriptions grow live.
func writeWorkingProtocolFiles(dir string, nodes []sub.Node) error {
	groups := groupProtocols(nodes)
	for _, protocol := range []string{"vless", "vmess", "trojan", "ss", "hysteria2", "tuic", "socks", "http", "unknown"} {
		group := groups[protocol]
		if len(group) == 0 {
			if err := os.Remove(filepath.Join(dir, protocol+".txt")); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			continue
		}
		if err := writeSubscription(filepath.Join(dir, protocol+".txt"), group); err != nil {
			return err
		}
	}
	return nil
}

// removeLegacyJSONArtifacts clears output snapshots from versions that exported
// Node structures as JSON. It only touches known SubWatch-generated names.
func removeLegacyJSONArtifacts(dir string) error {
	bases := []string{
		"all", "unique", "selected",
		"url-test-finished", "url-test-failed",
		"speedtest-finished", "speedtest-failed", "tested",
		"service-check-results", "final",
	}
	for _, base := range bases {
		paths := []string{filepath.Join(dir, base+".json")}
		splitPaths, err := filepath.Glob(filepath.Join(dir, base+"-*.json"))
		if err != nil {
			return err
		}
		paths = append(paths, splitPaths...)
		for _, path := range paths {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove %s: %w", path, err)
			}
		}
	}
	return nil
}

func groupProtocols(nodes []sub.Node) map[string][]sub.Node {
	groups := map[string][]sub.Node{}
	for _, n := range nodes {
		p := protocolOf(n)
		if p == "" {
			p = "unknown"
		}
		groups[p] = append(groups[p], n)
	}
	return groups
}

func clearProtocolSubscriptions(dir, base string) error {
	stale, err := filepath.Glob(filepath.Join(dir, base+"-*.txt"))
	if err != nil {
		return err
	}
	for _, path := range stale {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func writeProtocolSubscriptions(dir, base string, nodes []sub.Node) error {
	if err := clearProtocolSubscriptions(dir, base); err != nil {
		return err
	}
	for protocol, group := range groupProtocols(nodes) {
		if err := writeSubscription(filepath.Join(dir, base+"-"+protocol+".txt"), group); err != nil {
			return err
		}
	}
	return nil
}

func writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".subwatch-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		tmp.Close()
		if !ok {
			os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0600); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func writeSubscription(path string, nodes []sub.Node) error {
	var b strings.Builder
	for _, n := range nodes {
		b.WriteString(n.URI)
		b.WriteByte('\n')
	}
	return writeAtomic(path, []byte(b.String()))
}

func writeReport(path string, nodes []sub.Node) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"name", "protocol", "uri", "url_passed", "url_latency_ms", "url_error", "speed_passed", "download_mbps", "upload_mbps", "speed_error", "gemini_status", "gemini_note", "gemini_region", "service_error", "exit_ip", "country", "asn", "org", "ip_type", "ip_flags"})
	for _, n := range nodes {
		_ = w.Write([]string{n.Name, protocolOf(n), n.URI, strconv.FormatBool(n.URLPassed), strconv.FormatInt(n.URLLatencyMS, 10), n.URLError, strconv.FormatBool(n.SpeedPassed), fmt.Sprintf("%.3f", n.DownloadMbps), fmt.Sprintf("%.3f", n.UploadMbps), n.SpeedError, n.GeminiStatus, n.GeminiNote, n.GeminiRegion, n.ServiceError, n.ExitIP, n.Country, n.ASN, n.Org, n.IPType, flagsJSON(n.IPFlags)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return f.Sync()
}

func mergeResults(selected, urlResults, speedResults, serviceResults []sub.Node) []sub.Node {
	// URI can change after speed testing; the outbound key remains stable.
	identity := func(n sub.Node) string { if n.Key != "" { return n.Key }; return n.URI }
	byKey := make(map[string]sub.Node, len(selected))
	for _, n := range selected { byKey[identity(n)] = n }
	for _, group := range [][]sub.Node{urlResults, speedResults, serviceResults} {
		for _, n := range group { byKey[identity(n)] = n }
	}
	out := make([]sub.Node, 0, len(selected))
	for _, n := range selected { if updated, ok := byKey[identity(n)]; ok { out = append(out, updated) } }
	return out
}

func printFailureSummary(w io.Writer, title string, nodes []sub.Node, reason func(sub.Node) string) {
	if len(nodes) == 0 {
		return
	}
	counts := map[string]int{}
	for _, n := range nodes {
		r := strings.TrimSpace(reason(n))
		if r == "" {
			r = "unknown error"
		}
		counts[r]++
	}
	type item struct {
		reason string
		count  int
	}
	items := make([]item, 0, len(counts))
	for r, c := range counts {
		items = append(items, item{r, c})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count == items[j].count {
			return items[i].reason < items[j].reason
		}
		return items[i].count > items[j].count
	})
	fmt.Fprintf(w, "%s:\n", title)
	for i, it := range items {
		if i == 5 {
			fmt.Fprintf(w, "  ... and %d other reason(s)\n", len(items)-i)
			break
		}
		fmt.Fprintf(w, "  %dx %s\n", it.count, truncate(it.reason, 140))
	}
}

func flagsJSON(flags map[string]bool) string {
	if flags == nil {
		return "{}"
	}
	b, err := json.Marshal(flags)
	if err != nil {
		return "{}"
	}
	return string(b)
}
func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max-1]) + "…"
}
