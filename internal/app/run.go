package app

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"math"
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

	"golang.org/x/text/language"
	"golang.org/x/text/language/display"

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
	Reference  string
	Body       []byte
	Line       int
	ListPath   string
	LoadError  error
	ParseError string
}

type sourceQualityStats struct {
	Reference  string
	Line       int
	ListPath   string
	Status     string
	Eligible   int
	Exclusive  int
	Shared     int
	ExclusiveURLPassed   int
	SharedURLPassed      int
	ExclusiveSpeedPassed int
	SharedSpeedPassed    int
	ExclusiveGemini      int
	SharedGemini         int
}

type sourceQualityTotals struct {
	SourceAppearances    int
	GlobalUnique         int
	SharedUnique         int
	ExclusiveUnique      int
	ExclusiveURLPassed   int
	SharedURLPassed      int
	ExclusiveSpeedPassed int
	SharedSpeedPassed    int
	ExclusiveGemini      int
	SharedGemini         int
}

const (
	stabilityNameSuffix       = " 🧱"
	stableURLLatencyLimitMS   = int64(100)
	stableConsecutiveCycles   = 3
)

// StabilityTracker keeps consecutive fast URL-test results across complete
// runs in continuous mode. Its state intentionally lives only for one process.
type StabilityTracker struct {
	mu      sync.Mutex
	streaks map[string]int
}

func NewStabilityTracker() *StabilityTracker {
	return &StabilityTracker{streaks: make(map[string]int)}
}

func (tracker *StabilityTracker) recordCycle(results []sub.Node) map[string]int {
	if tracker == nil {
		tracker = NewStabilityTracker()
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	next := make(map[string]int, len(results))
	for _, node := range results {
		key := nodeIdentity(node)
		if key == "" {
			continue
		}
		streak := 0
		if node.URLPassed && node.URLLatencyMS >= 0 && node.URLLatencyMS < stableURLLatencyLimitMS {
			streak = tracker.streaks[key] + 1
		}
		next[key] = streak
	}
	// Replacing the map also resets nodes that disappeared from this cycle.
	tracker.streaks = next

	snapshot := make(map[string]int, len(next))
	for key, streak := range next {
		snapshot[key] = streak
	}
	return snapshot
}

func Run(ctx context.Context, opts Options, stdout, stderr io.Writer) error {
	return RunWithStability(ctx, opts, stdout, stderr, nil)
}

func RunWithStability(ctx context.Context, opts Options, stdout, stderr io.Writer, stability *StabilityTracker) (runErr error) {
	if stability == nil {
		stability = NewStabilityTracker()
	}
	if strings.TrimSpace(opts.OutDir) == "" {
		opts.OutDir = "out"
	}
	if err := os.MkdirAll(opts.OutDir, 0700); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	failures := newRunFailureLog()
	defer func() {
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			failures.Add("PROGRAM", runErr.Error())
		}
		if err := failures.Write(filepath.Join(opts.OutDir, "failed.txt")); err != nil {
			if runErr != nil {
				runErr = fmt.Errorf("%w (also failed to write failed.txt: %v)", runErr, err)
			} else {
				runErr = fmt.Errorf("write failed.txt: %w", err)
			}
		}
	}()

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
	if opts.IsolateSourceDownloads && opts.Interface == "auto" {
		return fmt.Errorf("network.isolate_source_downloads requires network.interface to name a physical interface")
	}
	if opts.Interface != "auto" {
		it, err := net.InterfaceByName(opts.Interface)
		if err != nil {
			return fmt.Errorf("network interface %q not found (run 'sw interfaces'): %w", opts.Interface, err)
		}
		if it.Flags&net.FlagUp == 0 {
			return fmt.Errorf("network interface %q exists but is DOWN", opts.Interface)
		}
		fmt.Fprintf(stdout, "[network] sing-box outbound binding: %s (UP)\n", it.Name)
	} else {
		fmt.Fprintln(stdout, "[network] sing-box outbound binding: automatic (system routing)")
	}
	if opts.Sources == "" {
		opts.Sources = "sources.txt"
	}
	if opts.SourceTimeout <= 0 {
		opts.SourceTimeout = sourceDownloadTimeout
	}
	if opts.OutDir == "" {
		opts.OutDir = "out"
	}
	if opts.URL == "" {
		opts.URL = "https://www.gstatic.com/generate_204"
	}
	if opts.SpeedProvider == "" {
		opts.SpeedProvider = "cloudflare"
	}
	opts.SpeedProvider = strings.ToLower(strings.TrimSpace(opts.SpeedProvider))
	if opts.SpeedProvider != "cloudflare" && opts.SpeedProvider != "ookla" {
		return fmt.Errorf("speed_test.provider must be cloudflare or ookla")
	}
	if opts.CloudflareFallback && opts.SpeedProvider != "ookla" {
		return fmt.Errorf("speed_test.cloudflare_fallback.enabled requires speed_test.provider=ookla")
	}
	if opts.MaxSpeedDuration <= 0 {
		opts.MaxSpeedDuration = DefaultOptions().MaxSpeedDuration
	}
	if opts.SpeedProvider == "ookla" && !opts.CloudflareFallback {
		// When Ookla runs alone, Cloudflare-only values have no effect.
		opts.FirstByteTimeout = 0
		opts.SpeedSampleInterval = 0
		opts.DownloadBytes = 0
		opts.UploadBytes = 0
		opts.SpeedDownloadURL = ""
		opts.SpeedUploadURL = ""
	} else {
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
		if opts.FirstByteTimeout <= 0 {
			opts.FirstByteTimeout = DefaultOptions().FirstByteTimeout
		}
		if opts.SpeedSampleInterval <= 0 {
			opts.SpeedSampleInterval = DefaultOptions().SpeedSampleInterval
		}
		if opts.DownloadBytes < 64*1024 || opts.UploadBytes < 64*1024 {
			return fmt.Errorf("download/upload test payload must be at least 64 KiB")
		}
		if opts.DownloadBytes > 100<<20 || opts.UploadBytes > 100<<20 {
			return fmt.Errorf("download/upload test payload cannot exceed 100 MiB")
		}
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
	// Keep the output directory focused on generated result files, including invalid input lines.
	// Intermediate selections, failed lists, CSV diagnostics and obsolete files
	// are deliberately not exported.
	if err := cleanOutputArtifacts(opts.OutDir); err != nil {
		return fmt.Errorf("clean output directory: %w", err)
	}

	// When explicitly enabled, create the namespace before loading remote source
	// URLs. The source list and local files remain host filesystem operations;
	// only HTTP(S) subscription downloads are executed inside this namespace.
	var sourceNamespace *core.NetworkNamespace
	if opts.IsolateSourceDownloads {
		if opts.Interface == "auto" {
			return fmt.Errorf("network.isolate_source_downloads requires network.interface to name a physical interface")
		}
		ns, nsErr := core.NewNetworkNamespace(ctx, opts.Interface, stdout)
		if nsErr != nil {
			return fmt.Errorf("prepare isolated source network: %w", nsErr)
		}
		if ns == nil {
			return fmt.Errorf("isolated source network was not created")
		}
		sourceNamespace = ns
		opts.NetworkNamespace = ns.Name
		opts.NetworkNamespaceInterface = ns.HostInterface()
		defer sourceNamespace.Close()
		fmt.Fprintf(stdout, "[network] remote subscription downloads isolated through %s\n", opts.Interface)
	}

	var sourceProgressRendered bool
	renderSourceProgress := func(progress sourceProgress) {
		sourceProgressRendered = true
		line := formatSourceProgressLine(progress, liveTerminalWidth(stdout))
		fmt.Fprintf(stdout, "\r\x1b[2K%s", line)
	}
	renderSourceProgress(sourceProgress{Reference: opts.Sources, Phase: "reading source list"})

	var sources []loadedSource
	var err error
	if sourceNamespace != nil {
		executable, exeErr := os.Executable()
		if exeErr != nil {
			return fmt.Errorf("locate SubWatch executable for isolated source downloads: %w", exeErr)
		}
		sources, err = loadSourcesWithFetcherProgress(ctx, opts.Sources, opts.SourceTimeout, func(fetchCtx context.Context, sourceURL string, timeout time.Duration) ([]byte, error) {
			return fetchSourceInNamespace(fetchCtx, sourceNamespace, executable, sourceURL, timeout)
		}, renderSourceProgress)
	} else {
		sources, err = loadSourcesWithFetcherProgress(ctx, opts.Sources, opts.SourceTimeout, fetchHTTPSource, renderSourceProgress)
	}
	if sourceProgressRendered {
		fmt.Fprintln(stdout)
		sourceProgressRendered = false
	}
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
	var invalidConfigs []string
	var sourceDiagnostics []string
	for i, source := range sources {
		if source.LoadError != nil {
			renderSourceProgress(sourceProgress{
				Index: i + 1, Total: len(sources), Loaded: loadedCount, Skipped: skippedCount,
				Reference: source.Reference, Phase: "skipping failed source",
			})
			sourceErrors++
			lineNo := source.Line
			if lineNo < 1 {
				lineNo = i + 1
			}
			diagnostic := fmt.Sprintf("source %d (%s): skipped: %v", lineNo, sourceProgressTitle(source.Reference), source.LoadError)
			sourceDiagnostics = append(sourceDiagnostics, diagnostic)
			failures.Add("SOURCE", diagnostic)
			continue
		}
		progress := sourceProgress{
			Index: i + 1, Total: len(sources), Loaded: loadedCount, Skipped: skippedCount,
			Reference: source.Reference, Phase: "parsing",
		}
		renderSourceProgress(progress)
		nodes, invalidLines, parseErr := sub.ParseDocumentWithDiagnostics(source.Body, func(processed, total, parsedNodes, invalidCount int) {
			progress.LinesProcessed, progress.TotalLines = processed, total
			progress.Nodes, progress.Invalid = parsedNodes, invalidCount
			renderSourceProgress(progress)
		})
		invalid += len(invalidLines)
		invalidConfigs = append(invalidConfigs, invalidLines...)
		if parseErr != nil {
			sources[i].ParseError = parseErr.Error()
			progress.Phase = "parse failed"
			renderSourceProgress(progress)
			sourceErrors++
			lineNo := source.Line
			if lineNo < 1 {
				lineNo = i + 1
			}
			diagnostic := fmt.Sprintf("source %d (%s): %v", lineNo, sourceProgressTitle(source.Reference), parseErr)
			sourceDiagnostics = append(sourceDiagnostics, diagnostic)
			failures.Add("SOURCE PARSE", diagnostic)
			continue
		}
		perSourceNodes[i] = nodes
		parsed = append(parsed, nodes...)
		progress.Phase = "parsed"
		progress.LinesProcessed = progress.TotalLines
		progress.Nodes = len(nodes)
		progress.Invalid = len(invalidLines)
		renderSourceProgress(progress)
	}
	if sourceProgressRendered {
		fmt.Fprintln(stdout)
	}
	for _, diagnostic := range sourceDiagnostics {
		fmt.Fprintln(stderr, diagnostic)
	}
	for i, line := range invalidConfigs {
		failures.Add("PARSER", fmt.Sprintf("rejected malformed or unsupported input #%d: %s", i+1, redactFailureInput(line)))
	}
	fmt.Fprintf(stdout, "Rejected input lines: %d | details: %s\n", len(invalidConfigs), filepath.Join(opts.OutDir, "failed.txt"))
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
	fmt.Fprintf(stdout, "Proxy interface: %s | URL concurrency: %d | speed concurrency: %d | service concurrency: %d\n", opts.Interface, opts.URLConcurrency, opts.SpeedConcurrency, opts.ServiceConcurrency)
	if opts.SpeedProvider == "ookla" {
		fmt.Fprintf(stdout, "Speed provider: Ookla Speedtest.net | automatic server selection | minimum DL/UL: %.2f/%.2f Mbps\n", opts.MinDownload, opts.MinUpload)
		if opts.CloudflareFallback {
			fmt.Fprintf(stdout, "Cloudflare fallback: enabled | payload: %.2f/%.2f MiB | same minimum DL/UL: %.2f/%.2f Mbps\n", float64(opts.DownloadBytes)/(1<<20), float64(opts.UploadBytes)/(1<<20), opts.MinDownload, opts.MinUpload)
		}
	} else {
		fmt.Fprintf(stdout, "Speed provider: Cloudflare | payload: %.2f/%.2f MiB | minimum DL/UL: %.2f/%.2f Mbps | download URL: %s | upload URL: %s\n", float64(opts.DownloadBytes)/(1<<20), float64(opts.UploadBytes)/(1<<20), opts.MinDownload, opts.MinUpload, opts.SpeedDownloadURL, opts.SpeedUploadURL)
	}

	fmt.Fprintln(stdout, "[prepare] Checking sing-box; first run may download the verified core...")
	corePath, err := core.FindOrDownloadProgress(opts.CorePath, stdout)
	if err != nil {
		return fmt.Errorf("sing-box: %w", err)
	}
	opts.CorePath = corePath
	fmt.Fprintf(stdout, "[prepare] sing-box ready: %s\n", corePath)
	if opts.Interface != "auto" && opts.NetworkNamespace == "" {
		ns, nsErr := core.NewNetworkNamespace(ctx, opts.Interface, stdout)
		if nsErr != nil { return fmt.Errorf("prepare isolated network: %w", nsErr) }
		if ns != nil {
			defer ns.Close()
			opts.NetworkNamespace = ns.Name
			opts.NetworkNamespaceInterface = ns.HostInterface()
		}
	}

	// Stage 1: every selected unique node gets the URL test. Nothing is speed-tested here.
	fmt.Fprintf(stdout, "\n[2/4] URL TEST: %d nodes, target %s, timeout %s\n", len(selected), opts.URL, opts.URLTimeout)
	var urlPassedLive []sub.Node
	lastURLSnapshot := time.Now()
	completedSinceURLSnapshot := 0
	if err := writeStageFiles(opts.OutDir, "url-tested", nil, true); err != nil {
		return fmt.Errorf("initialize URL result files: %w", err)
	}
	urlResults, stageErr := runStageWithLiveCallback(ctx, "URL TEST", selected, opts.URLConcurrency, stdout, func(n *sub.Node) bool { return n.URLPassed }, func(stageCtx context.Context, n *sub.Node, live func(sub.Node, string)) {
		live(*n, "checking URL…")
		passed, latency, testErr := runURLTest(stageCtx, *n, opts)
		n.URLPassed, n.URLLatencyMS = passed, latency
		if testErr != nil {
			n.URLError = testErr.Error()
		} else if !passed {
			n.URLError = "URL test returned non-success status"
		}
		if opts.RenameNodes {
			if passed {
				n.RenameError = renameNodeAfterURL(stageCtx, n, opts)
			} else if err := sub.RenameNode(n, urlStageNodeName("")); err != nil {
				n.RenameError = err.Error()
			}
		}
		live(*n, urlLiveDetail(*n))
	}, func(completed sub.Node) error {
		completedSinceURLSnapshot++
		if completed.URLPassed {
			urlPassedLive = append(urlPassedLive, completed)
		} else {
			failures.AddNode("URL TEST", completed, completed.URLError)
		}
		if completed.RenameError != "" {
			failures.AddNode("RENAME", completed, completed.RenameError)
		}
		if !shouldRefreshStageSnapshot(completedSinceURLSnapshot, time.Now(), lastURLSnapshot) {
			return nil
		}
		if err := writeStageFiles(opts.OutDir, "url-tested", urlPassedLive, true); err != nil {
			return fmt.Errorf("refresh URL result files: %w", err)
		}
		lastURLSnapshot = time.Now()
		completedSinceURLSnapshot = 0
		return nil
	})
	if stageErr != nil {
		return stageErr
	}
	urlPassed := filterNodes(urlResults, func(n sub.Node) bool { return n.URLPassed })
	// Keep every selected node's remark synchronized in all.txt too. Nodes
	// that fail URL reachability cannot have a meaningful country/speed result,
	// so they receive the explicit 🌐 Unknown fallback rather than the source name.
	if opts.RenameNodes {
		applyNodeUpdatesByIdentity(parsed, urlResults)
		if err := writeSubscription(filepath.Join(opts.OutDir, "all.txt"), parsed); err != nil {
			return err
		}
	}
	if err := writeStageFiles(opts.OutDir, "url-tested", urlPassed, true); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "URL test complete: %d passed / %d failed. Result files refresh every 5s or 100 completed configs.\n", len(urlPassed), len(urlResults)-len(urlPassed))

	// Stage 2 begins only after every URL test has completed (global barrier).
	// Every URL-passed node is tested by the primary provider exactly once.
	fmt.Fprintf(stdout, "\n[3/4] SPEED TEST (%s): %d URL-passed nodes, timeout %s, minimum DL/UL %.2f/%.2f Mbps\n", strings.ToUpper(opts.SpeedProvider), len(urlPassed), opts.SpeedTimeout, opts.MinDownload, opts.MinUpload)
	speedResults, _ := runStageWithLiveCallback(ctx, "SPEED TEST", urlPassed, opts.SpeedConcurrency, stdout, func(n *sub.Node) bool { return n.SpeedPassed }, func(stageCtx context.Context, n *sub.Node, live func(sub.Node, string)) {
		live(*n, speedLiveDetail(*n, "starting "+strings.ToUpper(opts.SpeedProvider)+" speed test…"))
		down, up, testErr := runSpeedTestWithProgress(stageCtx, *n, opts, func(progress check.SpeedProgress) {
			live(*n, speedProgressDetail(*n, progress))
		})
		recordSpeedProviderResult(n, opts.SpeedProvider, down, up, testErr, opts)

		// When fallback is enabled, defer renaming Ookla failures until their
		// Cloudflare measurement has completed. This keeps the displayed rate
		// tied to the provider that supplies the final result.
		if opts.RenameNodes && (!opts.CloudflareFallback || n.SpeedPassed) {
			n.RenameError = renameNodeAfterSpeed(stageCtx, n, opts, down, up)
		}
		state := "PASS"
		if !n.SpeedPassed {
			state = "FAILED: " + shortLiveError(n.SpeedError)
		} else {
			state = "PASS (" + strings.ToUpper(opts.SpeedProvider) + ")"
		}
		if n.RenameError != "" {
			state += " | rename: " + shortLiveError(n.RenameError)
		}
		live(*n, speedLiveDetail(*n, state))
	}, nil)

	var fallbackResults []sub.Node
	if opts.CloudflareFallback {
		fallbackCandidates := filterNodes(speedResults, func(n sub.Node) bool {
			return needsCloudflareFallback(n, opts.CloudflareFallback)
		})
		fmt.Fprintf(stdout, "\n[3b/4] CLOUDFLARE FALLBACK: %d Ookla-failed URL-passed nodes, timeout %s\n", len(fallbackCandidates), opts.SpeedTimeout)
		fallbackResults, _ = runStageWithLiveCallback(ctx, "CLOUDFLARE FALLBACK", fallbackCandidates, opts.SpeedConcurrency, stdout, func(n *sub.Node) bool { return n.CloudflarePassed }, func(stageCtx context.Context, n *sub.Node, live func(sub.Node, string)) {
			live(*n, speedLiveDetail(*n, "starting Cloudflare fallback…"))
			down, up, testErr := runCloudflareSpeedTestWithProgress(stageCtx, *n, opts, func(progress check.SpeedProgress) {
				live(*n, speedProgressDetail(*n, progress))
			})
			recordSpeedProviderResult(n, "cloudflare", down, up, testErr, opts)
			if opts.RenameNodes {
				n.RenameError = renameNodeAfterSpeed(stageCtx, n, opts, down, up)
			}
			state := "PASS (CLOUDFLARE)"
			if !n.CloudflarePassed {
				state = "FAILED: " + shortLiveError(n.CloudflareError)
			}
			if n.RenameError != "" {
				state += " | rename: " + shortLiveError(n.RenameError)
			}
			live(*n, speedLiveDetail(*n, state))
		}, nil)
		speedResults = mergeResults(speedResults, nil, fallbackResults, nil)
		cloudflareFallbackPassed := filterNodes(fallbackResults, func(n sub.Node) bool { return n.CloudflarePassed })
		fmt.Fprintf(stdout, "Cloudflare fallback complete: %d rescued / %d tested; rescued nodes are included in speedtested.txt.\n", len(cloudflareFallbackPassed), len(fallbackResults))
	}

	// Experimental gaming checks are deliberately independent of the speed
	// provider and fallback result: every URL-passed node gets this stage.
	gamingPassedByKey := make(map[string]bool)
	if opts.GamingQuality.Enabled {
		fmt.Fprintf(stdout, "\n[3c/4] EXPERIMENTAL GAMING QUALITY: %d URL-passed nodes via Cloudflare, timeout %s\n", len(urlPassed), opts.GamingQuality.Timeout)
		gamingResults, gamingStageErr := runStageWithLiveCallback(ctx, "GAMING QUALITY", urlPassed, opts.GamingQuality.Concurrency, stdout,
			func(n *sub.Node) bool { return n.GamingPassed },
			func(stageCtx context.Context, n *sub.Node, live func(sub.Node, string)) {
				n.GamingTested = true
				var quality check.NetworkQuality
				testErr := withNodeClient(stageCtx, *n, opts, opts.GamingQuality.Timeout, func(nodeCtx context.Context, client *http.Client) error {
					testCtx, cancel := context.WithTimeout(nodeCtx, opts.GamingQuality.Timeout)
					defer cancel()
					var qualityErr error
					quality, qualityErr = check.CloudflareNetworkQualityTest(
						testCtx, client,
						"https://speed.cloudflare.com/__down?bytes={bytes}",
						"https://speed.cloudflare.com/__up",
						opts.GamingQuality.DownloadBytes,
						opts.GamingQuality.UploadBytes,
						opts.GamingQuality.LatencySamples,
						opts.GamingQuality.SampleInterval,
					)
					return qualityErr
				})
				if testErr != nil {
					n.GamingPassed = false
					n.GamingError = testErr.Error()
					live(*n, "Gaming quality: FAILED | "+shortLiveError(n.GamingError))
					return
				}
				n.GamingLatencyMS = quality.LatencyMS
				n.GamingJitterMS = quality.JitterMS
				n.GamingDownloadLoadedLatencyMS = quality.DownloadLoadedLatencyMS
				n.GamingDownloadLoadedJitterMS = quality.DownloadLoadedJitterMS
				n.GamingUploadLoadedLatencyMS = quality.UploadLoadedLatencyMS
				n.GamingUploadLoadedJitterMS = quality.UploadLoadedJitterMS
				n.GamingPassed, n.GamingError = gamingQualityDecision(quality, opts.GamingQuality)
				live(*n, gamingQualityLiveDetail(quality, n.GamingPassed, n.GamingError))
			},
			func(completed sub.Node) error {
				if completed.GamingPassed {
					gamingPassedByKey[nodeIdentity(completed)] = true
				} else if completed.GamingError != "" {
					failures.AddNode("GAMING QUALITY", completed, completed.GamingError)
				}
				return nil
			},
		)
		if gamingStageErr != nil {
			return gamingStageErr
		}
		speedResults = mergeResults(speedResults, nil, gamingResults, nil)
		gamingPassed := filterNodes(gamingResults, func(n sub.Node) bool { return n.GamingPassed })
		if err := writeStageFiles(opts.OutDir, "gaming-quality", gamingPassed, true); err != nil {
			return fmt.Errorf("write gaming-quality results: %w", err)
		}
		if err := writeGamingQualityReport(opts.OutDir, gamingResults); err != nil {
			return fmt.Errorf("write gaming-quality report: %w", err)
		}
		fmt.Fprintf(stdout, "Gaming quality complete: %d candidates / %d tested. Packet loss is not measured in this experimental HTTP-only mode.\n", len(gamingPassed), len(gamingResults))
	}

	// Speed testing may rename nodes after the URL-stage file was first written.
	// Propagate final provider measurements and remarks by stable node identity.
	applyNodeUpdatesByIdentity(parsed, speedResults)
	applyNodeUpdatesByIdentity(urlPassed, speedResults)
	if err := writeSubscription(filepath.Join(opts.OutDir, "all.txt"), parsed); err != nil {
		return err
	}
	if err := writeStageFiles(opts.OutDir, "url-tested", urlPassed, true); err != nil {
		return err
	}

	speedPassed := filterNodes(speedResults, func(n sub.Node) bool { return n.SpeedPassed })
	speedFailed := filterNodes(speedResults, func(n sub.Node) bool { return !n.SpeedPassed })
	for _, node := range speedFailed {
		reason := node.SpeedError
		if reason == "" {
			reason = "speed test failed without an error message"
		}
		failures.AddNode("SPEED TEST", node, reason)
	}
	for _, node := range speedResults {
		if node.OoklaTested && !node.OoklaPassed {
			stage := "OOKLA"
			if node.CloudflareTested && node.CloudflarePassed {
				stage = "CLOUDFLARE RECOVERY"
			}
			reason := node.OoklaError
			if reason == "" {
				reason = "Ookla test failed"
			}
			failures.AddNode(stage, node, reason)
		}
		if node.CloudflareTested && !node.CloudflarePassed {
			reason := node.CloudflareError
			if reason == "" {
				reason = "Cloudflare fallback failed"
			}
			failures.AddNode("CLOUDFLARE", node, reason)
		}
		if node.RenameError != "" {
			failures.AddNode("RENAME", node, node.RenameError)
		}
	}
	if err := writeStageFiles(opts.OutDir, "speedtested", speedPassed, true); err != nil {
		return err
	}
	if opts.CloudflareFallback {
		ooklaPassed := filterNodes(speedResults, func(n sub.Node) bool { return n.OoklaTested && n.OoklaPassed })
		cloudflareRescued := filterNodes(speedResults, func(n sub.Node) bool { return n.OoklaTested && !n.OoklaPassed && n.CloudflareTested && n.CloudflarePassed })
		fmt.Fprintf(stdout, "Speed pipeline complete: %d passed / %d failed | Ookla passed: %d | Cloudflare rescued: %d.\n", len(speedPassed), len(speedFailed), len(ooklaPassed), len(cloudflareRescued))
	} else {
		fmt.Fprintf(stdout, "Speed test complete: %d passed / %d failed.\n", len(speedPassed), len(speedFailed))
	}
	// Stage 3 starts only after every speed test has completed. No service check
	// is run on a node that failed either speed threshold.
	fmt.Fprintf(stdout, "\n[4/4] SERVICE CHECK: %d speed-passed nodes, Gemini must be fully available\n", len(speedPassed))
	serviceResults, _ := runStageWithLiveCallback(ctx, "SERVICE CHECK", speedPassed, opts.ServiceConcurrency, stdout, func(n *sub.Node) bool { return geminiAvailable(*n) }, func(stageCtx context.Context, n *sub.Node, live func(sub.Node, string)) {
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
	for _, node := range serviceResults {
		if node.ServiceError != "" {
			failures.AddNode("SERVICE CHECK", node, node.ServiceError)
		} else if !geminiAvailable(node) {
			reason := strings.TrimSpace(node.GeminiStatus)
			if node.GeminiNote != "" {
				reason += ": " + node.GeminiNote
			}
			if reason == "" {
				reason = "Gemini was not classified as available"
			}
			failures.AddNode("GEMINI", node, reason)
		}
	}
	finalCandidates := filterNodes(serviceResults, func(n sub.Node) bool { return geminiAvailable(n) })
	// Each node is tested exactly once per enabled stage: URL, speed, then Gemini.
	// There is deliberately no final recheck stage; it duplicated URL and Gemini requests.
	finalNodes := finalCandidates
	if err := writeGeminiResults(opts.OutDir, finalNodes, parsed, urlPassed, speedPassed, false); err != nil {
		return err
	}

	// Stability is based only on successful URL checks under 100 ms. A node
	// receives 🧱 from its second consecutive qualifying full cycle onward.
	streaks := stability.recordCycle(urlResults)
	if err := writeStabilityResults(opts.OutDir, parsed, urlPassed, speedPassed, finalNodes, streaks, gamingPassedByKey); err != nil {
		return fmt.Errorf("write stability markers: %w", err)
	}
	stableCount := 0
	for _, streak := range streaks {
		if streak >= stableConsecutiveCycles {
			stableCount++
		}
	}
	fmt.Fprintf(stdout, "[stability] %d nodes marked 🧱 (at least %d consecutive URL checks below %d ms)\n", stableCount, stableConsecutiveCycles, stableURLLatencyLimitMS)

	fmt.Fprintf(stdout, "\nFinished. Parsed: %d | unique: %d | URL passed: %d | speed passed: %d | Gemini available: %d\n", len(parsed), len(unique), len(urlPassed), len(speedPassed), len(finalNodes))
	perSourceEligible := make([][]sub.Node, len(perSourceNodes))
	for i := range perSourceNodes {
		perSourceEligible[i] = filterProtocols(perSourceNodes[i], opts.Protocols)
	}
	printSubscriptionQualitySummary(stdout, sources, perSourceEligible, urlPassed, speedPassed, finalNodes)
	fmt.Fprintf(stdout, "Output directory: %s\n", opts.OutDir)
	if err := failures.Write(filepath.Join(opts.OutDir, "failed.txt")); err != nil {
		return fmt.Errorf("write failure log: %w", err)
	}
	fmt.Fprintf(stdout, "Failure log: %d entries -> %s\n", failures.Count(), filepath.Join(opts.OutDir, "failed.txt"))
	fmt.Fprintln(stdout, "Output: all.txt, url-tested*.txt, speedtested*.txt, failed.txt")
	if opts.RemoteUpload.Enabled {
		if uploadErr := uploadOutput(ctx, opts.RemoteUpload, opts.OutDir, stdout, stderr); uploadErr != nil {
			failures.Add("UPLOAD", uploadErr.Error())
			fmt.Fprintf(stderr, "[UPLOAD] ERROR: %v\n", uploadErr)
		} else {
			fmt.Fprintln(stdout, "[UPLOAD] Finished without reported errors")
		}
	} else {
		fmt.Fprintln(stdout, "[UPLOAD] Skipped: remote_upload.enabled is false in the loaded config")
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
type sourceFetcher func(context.Context, string, time.Duration) ([]byte, error)

type sourceProgress struct {
	Index, Total       int
	Loaded, Skipped    int
	Reference, Phase   string
	LinesProcessed     int
	TotalLines         int
	Nodes, Invalid     int
}

func loadSourcesWithTimeout(ctx context.Context, path string, timeout time.Duration) ([]loadedSource, error) {
	return loadSourcesWithFetcher(ctx, path, timeout, fetchHTTPSource)
}

func loadSourcesWithFetcher(ctx context.Context, path string, timeout time.Duration, fetch sourceFetcher) ([]loadedSource, error) {
	return loadSourcesWithFetcherProgress(ctx, path, timeout, fetch, nil)
}

func loadSourcesWithFetcherProgress(ctx context.Context, path string, timeout time.Duration, fetch sourceFetcher, progress func(sourceProgress)) ([]loadedSource, error) {
	if fetch == nil {
		fetch = fetchHTTPSource
	}
	report := func(p sourceProgress) {
		if progress != nil {
			progress(p)
		}
	}
	report(sourceProgress{Reference: path, Phase: "reading source list"})
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(data) > 2<<20 {
		return nil, fmt.Errorf("%s exceeds 2 MiB", path)
	}
	var entries []struct {
		line int
		text string
	}
	for lineNo, line := range strings.Split(string(data), "\n") {
		entry := strings.TrimSpace(strings.TrimPrefix(line, "\ufeff"))
		if entry == "" || strings.HasPrefix(entry, "#") {
			continue
		}
		entries = append(entries, struct {
			line int
			text string
		}{line: lineNo + 1, text: entry})
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%s contains no sources", path)
	}

	var sources []loadedSource
	loadedCount, skippedCount := 0, 0
	for index, item := range entries {
		if err := ctx.Err(); err != nil {
			return sources, err
		}
		entry := item.text
		source := loadedSource{Reference: entry, Line: item.line, ListPath: path}
		phase := "reading local file"
		if strings.HasPrefix(entry, "http://") || strings.HasPrefix(entry, "https://") {
			phase = "downloading"
		} else if strings.Contains(entry, "://") {
			phase = "loading inline link"
		}
		report(sourceProgress{Index: index + 1, Total: len(entries), Loaded: loadedCount, Skipped: skippedCount, Reference: entry, Phase: phase})
		if phase == "downloading" {
			u, parseErr := url.Parse(entry)
			if parseErr != nil || u.Host == "" {
				source.LoadError = fmt.Errorf("invalid subscription URL")
			} else {
				body, fetchErr := fetch(ctx, entry, timeout)
				if fetchErr != nil {
					if ctx.Err() != nil {
						return sources, ctx.Err()
					}
					source.LoadError = fmt.Errorf("download failed: %w", fetchErr)
				} else {
					source.Body = body
				}
			}
		} else if phase == "loading inline link" {
			source.Body = []byte(entry)
		} else {
			local, readErr := os.ReadFile(entry)
			if readErr != nil {
				source.LoadError = fmt.Errorf("read local source: %w", readErr)
			} else {
				source.Body = local
			}
		}
		if source.LoadError != nil {
			skippedCount++
			report(sourceProgress{Index: index + 1, Total: len(entries), Loaded: loadedCount, Skipped: skippedCount, Reference: entry, Phase: "failed"})
		} else {
			loadedCount++
			finishedPhase := "loaded"
			if phase == "downloading" {
				finishedPhase = "downloaded"
			}
			report(sourceProgress{Index: index + 1, Total: len(entries), Loaded: loadedCount, Skipped: skippedCount, Reference: entry, Phase: finishedPhase})
		}
		sources = append(sources, source)
	}
	report(sourceProgress{Index: len(entries), Total: len(entries), Loaded: loadedCount, Skipped: skippedCount, Phase: "all sources loaded"})
	return sources, nil
}

func fetchHTTPSource(ctx context.Context, sourceURL string, timeout time.Duration) ([]byte, error) {
	return fetchHTTPSourceWithProxy(ctx, sourceURL, timeout, true)
}

// fetchHTTPSourceDirect deliberately ignores HTTP_PROXY/HTTPS_PROXY inherited
// from the host: an isolated source download must use the namespace's own route.
func fetchHTTPSourceDirect(ctx context.Context, sourceURL string, timeout time.Duration) ([]byte, error) {
	return fetchHTTPSourceWithProxy(ctx, sourceURL, timeout, false)
}

func fetchHTTPSourceWithProxy(ctx context.Context, sourceURL string, timeout time.Duration, useEnvironmentProxy bool) ([]byte, error) {
	if timeout <= 0 {
		timeout = sourceDownloadTimeout
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", "SubWatch/0.3.9")
	client := &http.Client{Timeout: timeout}
	if !useEnvironmentProxy {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		client.Transport = transport
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("server returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if len(body) > 8<<20 {
		return nil, fmt.Errorf("subscription exceeds 8 MiB")
	}
	return body, nil
}

func fetchSourceInNamespace(ctx context.Context, namespace *core.NetworkNamespace, executable, sourceURL string, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		timeout = sourceDownloadTimeout
	}
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := namespace.Command(fetchCtx, executable, "__fetch-source", "--timeout", timeout.String())
	cmd.Stdin = strings.NewReader(sourceURL + "\n")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	body, err := cmd.Output()
	if err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return nil, fmt.Errorf("isolated source downloader: %w: %s", err, detail)
		}
		return nil, fmt.Errorf("isolated source downloader: %w", err)
	}
	if len(body) > 8<<20 {
		return nil, fmt.Errorf("isolated source response exceeds 8 MiB")
	}
	return body, nil
}

// FetchSourceCommand is an internal entry point invoked by the parent process
// through ip netns exec. The URL is read from stdin so credentials are not
// copied into the helper process command line.
func FetchSourceCommand(timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("source download timeout must be positive")
	}
	input, err := io.ReadAll(io.LimitReader(os.Stdin, 4097))
	if err != nil {
		return fmt.Errorf("read source URL: %w", err)
	}
	if len(input) > 4096 {
		return fmt.Errorf("source URL is too long")
	}
	sourceURL := strings.TrimSpace(string(input))
	parsed, err := url.Parse(sourceURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("invalid HTTP(S) source URL")
	}
	body, err := fetchHTTPSourceDirect(context.Background(), sourceURL, timeout)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(body)
	return err
}

func sourceProgressTitle(reference string) string {
	reference = strings.TrimSpace(reference)
	if parsed, err := url.Parse(reference); err == nil && parsed.Scheme != "" {
		scheme := strings.ToLower(parsed.Scheme)
		switch scheme {
		case "http", "https":
			if parsed.Host != "" {
				return parsed.Host
			}
		case "vless", "trojan", "ss", "shadowsocks", "hysteria2", "hy2", "tuic", "socks", "socks5":
			if parsed.Host != "" {
				return scheme + "://" + parsed.Host
			}
		case "vmess":
			return "vmess link"
		}
		return scheme + " source"
	}
	base := filepath.Base(reference)
	if base == "." || base == string(filepath.Separator) || base == "" {
		return "subscription source"
	}
	return base
}

func formatSourceProgressLine(progress sourceProgress, terminalWidth int) string {
	identity := cleanLiveText(sourceProgressTitle(progress.Reference))
	head := "[PARSE]"
	if progress.Total > 0 {
		head = fmt.Sprintf("[PARSE %d/%d ok:%d skip:%d]", progress.Index, progress.Total, progress.Loaded, progress.Skipped)
	}
	detail := cleanLiveText(progress.Phase)
	if progress.Phase == "parsing" || progress.Phase == "parsed" {
		verb := "lines"
		if progress.Phase == "parsed" {
			verb = "parsed"
		}
		detail = fmt.Sprintf("%s %d/%d | nodes %d | bad %d", verb, progress.LinesProcessed, progress.TotalLines, progress.Nodes, progress.Invalid)
	}
	line := strings.TrimSpace(head + " | " + identity + " | " + detail)
	if terminalWidth > 0 {
		limit := terminalWidth - 1
		if limit < 1 {
			limit = 1
		}
		return truncateDisplay(line, limit)
	}
	return line
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

func nodeKeySet(nodes []sub.Node) map[string]struct{} {
	keys := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		keys[nodeDedupKey(node)] = struct{}{}
	}
	return keys
}

func sourceSummaryLabel(source loadedSource) string {
	label := sourceProgressTitle(source.Reference)
	if parsed, err := url.Parse(source.Reference); err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") {
		// Keep the host, but never print URL credentials, paths or subscription tokens.
		label = parsed.Host
	} else if !strings.Contains(source.Reference, "://") {
		label = filepath.Clean(source.Reference)
	}
	if source.Line > 0 {
		listName := filepath.Base(source.ListPath)
		if listName == "" || listName == "." {
			listName = "sources.txt"
		}
		return fmt.Sprintf("%s [%s:%d]", label, listName, source.Line)
	}
	return label
}

func sourceStageRate(passed, tested int) string {
	if tested == 0 {
		return fmt.Sprintf("%d/0 (n/a)", passed)
	}
	return fmt.Sprintf("%d/%d (%.1f%%)", passed, tested, 100*float64(passed)/float64(tested))
}

const (
	stageSnapshotEvery    = 100
	stageSnapshotInterval = 5 * time.Second
)

func shouldRefreshStageSnapshot(completed int, now, last time.Time) bool {
	return completed >= stageSnapshotEvery || now.Sub(last) >= stageSnapshotInterval
}

// buildSubscriptionSourceStats globally deduplicates each source before attribution.
// Shared configs are counted once in the global shared funnel and are never added
// to any source's independent contribution.
func buildSubscriptionSourceStats(
	sources []loadedSource,
	perSourceNodes [][]sub.Node,
	urlPassed, speedPassed, geminiAvailable []sub.Node,
) ([]sourceQualityStats, sourceQualityTotals) {
	perSourceKeys := make([][]string, len(sources))
	membership := make(map[string]int)
	totals := sourceQualityTotals{}

	for sourceIndex := range sources {
		if sourceIndex >= len(perSourceNodes) {
			continue
		}
		seen := make(map[string]struct{}, len(perSourceNodes[sourceIndex]))
		for _, node := range perSourceNodes[sourceIndex] {
			key := nodeDedupKey(node)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			perSourceKeys[sourceIndex] = append(perSourceKeys[sourceIndex], key)
			membership[key]++
			totals.SourceAppearances++
		}
	}

	totals.GlobalUnique = len(membership)
	urlKeys := nodeKeySet(urlPassed)
	speedKeys := nodeKeySet(speedPassed)
	geminiKeys := nodeKeySet(geminiAvailable)
	for key, sourceCount := range membership {
		_, urlOK := urlKeys[key]
		_, speedOK := speedKeys[key]
		_, geminiOK := geminiKeys[key]
		if sourceCount > 1 {
			totals.SharedUnique++
			if urlOK { totals.SharedURLPassed++ }
			if speedOK { totals.SharedSpeedPassed++ }
			if geminiOK { totals.SharedGemini++ }
		} else {
			totals.ExclusiveUnique++
			if urlOK { totals.ExclusiveURLPassed++ }
			if speedOK { totals.ExclusiveSpeedPassed++ }
			if geminiOK { totals.ExclusiveGemini++ }
		}
	}

	stats := make([]sourceQualityStats, len(sources))
	for sourceIndex, source := range sources {
		item := sourceQualityStats{
			Reference: source.Reference,
			Line: source.Line,
			ListPath: source.ListPath,
			Eligible: len(perSourceKeys[sourceIndex]),
		}
		switch {
		case source.LoadError != nil:
			item.Status = "load failed"
		case source.ParseError != "":
			item.Status = "parse failed"
		}
		for _, key := range perSourceKeys[sourceIndex] {
			shared := membership[key] > 1
			if shared {
				item.Shared++
			} else {
				item.Exclusive++
			}
			_, urlOK := urlKeys[key]
			_, speedOK := speedKeys[key]
			_, geminiOK := geminiKeys[key]
			if shared {
				if urlOK { item.SharedURLPassed++ }
				if speedOK { item.SharedSpeedPassed++ }
				if geminiOK { item.SharedGemini++ }
			} else {
				if urlOK { item.ExclusiveURLPassed++ }
				if speedOK { item.ExclusiveSpeedPassed++ }
				if geminiOK { item.ExclusiveGemini++ }
			}
		}
		stats[sourceIndex] = item
	}

	// Rank by new, source-exclusive speed-passed configs. Shared results are
	// excluded because the same config can appear in multiple subscriptions.
	sort.SliceStable(stats, func(i, j int) bool {
		a, b := stats[i], stats[j]
		if a.ExclusiveSpeedPassed != b.ExclusiveSpeedPassed {
			return a.ExclusiveSpeedPassed > b.ExclusiveSpeedPassed
		}
		if a.ExclusiveURLPassed != b.ExclusiveURLPassed {
			return a.ExclusiveURLPassed > b.ExclusiveURLPassed
		}
		aRate, bRate := 0.0, 0.0
		if a.ExclusiveURLPassed > 0 { aRate = float64(a.ExclusiveSpeedPassed) / float64(a.ExclusiveURLPassed) }
		if b.ExclusiveURLPassed > 0 { bRate = float64(b.ExclusiveSpeedPassed) / float64(b.ExclusiveURLPassed) }
		if aRate != bRate { return aRate > bRate }
		return a.Exclusive > b.Exclusive
	})
	return stats, totals
}

func printSubscriptionQualitySummary(
	w io.Writer,
	sources []loadedSource,
	perSourceNodes [][]sub.Node,
	urlPassed, speedPassed, geminiAvailable []sub.Node,
) {
	stats, totals := buildSubscriptionSourceStats(
		sources, perSourceNodes, urlPassed, speedPassed, geminiAvailable,
	)
	fmt.Fprintln(w, "\nSubscription source quality:")
	fmt.Fprintf(w, "Source occurrences: %d | globally unique enabled configs: %d | repeated appearances: %d | configs shared by sources: %d\n",
		totals.SourceAppearances, totals.GlobalUnique, totals.SourceAppearances-totals.GlobalUnique, totals.SharedUnique,
	)
	fmt.Fprintf(w, "Source-exclusive (%d): URL %s -> speed %s -> Gemini %s\n",
		totals.ExclusiveUnique,
		sourceStageRate(totals.ExclusiveURLPassed, totals.ExclusiveUnique),
		sourceStageRate(totals.ExclusiveSpeedPassed, totals.ExclusiveURLPassed),
		sourceStageRate(totals.ExclusiveGemini, totals.ExclusiveSpeedPassed),
	)
	fmt.Fprintf(w, "Shared globally (%d, counted once): URL %s -> speed %s -> Gemini %s\n",
		totals.SharedUnique,
		sourceStageRate(totals.SharedURLPassed, totals.SharedUnique),
		sourceStageRate(totals.SharedSpeedPassed, totals.SharedURLPassed),
		sourceStageRate(totals.SharedGemini, totals.SharedSpeedPassed),
	)
	fmt.Fprintln(w, "Sources ranked by new speed-passed configs (exclusive only):")
	for rank, item := range stats {
		label := sourceSummaryLabel(loadedSource{Reference: item.Reference, Line: item.Line, ListPath: item.ListPath})
		status := ""
		if item.Status != "" { status = " [" + item.Status + "]" }
		fmt.Fprintf(w, "%2d. %s | only here %d | URL %s | speed %s | Gemini %s%s\n",
			rank+1, label, item.Exclusive,
			sourceStageRate(item.ExclusiveURLPassed, item.Exclusive),
			sourceStageRate(item.ExclusiveSpeedPassed, item.ExclusiveURLPassed),
			sourceStageRate(item.ExclusiveGemini, item.ExclusiveSpeedPassed),
			status,
		)
	}
	if len(stats) == 0 {
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
	// Existing configs commonly enabled "hysteria2" before legacy Hysteria
	// URI support was added. Treat that setting as enabling both Hysteria
	// generations while still exporting their distinct protocol names.
	if allowed["hysteria2"] {
		allowed["hysteria"] = true
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
	type workerStatus struct {
		node   sub.Node
		detail string
		active bool
	}
	statuses := make([]workerStatus, concurrency)
	for i := range statuses {
		statuses[i] = workerStatus{
			node:   sub.Node{Name: fmt.Sprintf("Worker %d", i+1)},
			detail: "waiting…",
		}
	}

	terminalWidth := liveTerminalWidth(stdout)
	terminalRowsDrawn := false
	render := func() {
		if terminalWidth > 0 {
			active := 0
			for _, status := range statuses {
				if status.active {
					active++
				}
			}
			header := fmt.Sprintf("[%s] %d/%d | active: %d | passed: %d | failed: %d",
				label, done, len(nodes), active, successes, done-successes)
			if terminalRowsDrawn {
				fmt.Fprintf(stdout, "\x1b[%dA", concurrency+1)
			}
			fmt.Fprintf(stdout, "\r\x1b[2K%s\n", truncateDisplay(header, terminalWidth-1))
			for i, status := range statuses {
				line := formatLiveWorkerStatusLine(label, i+1, status.node, status.detail, terminalWidth)
				fmt.Fprintf(stdout, "\r\x1b[2K%s\n", line)
			}
			terminalRowsDrawn = true
			return
		}

		// When stdout is not a terminal, preserve the compact single-line log
		// behavior; the terminal UI above gets one independently updated row
		// for every concurrent worker.
		visible := 0
		for i := range statuses {
			if statuses[i].active {
				visible = i
			}
		}
		line := formatLiveStatusLine(label, done, len(nodes), successes, statuses[visible].node, statuses[visible].detail, 0)
		fmt.Fprintf(stdout, "\r\x1b[2K%s", line)
	}

	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			for i := range jobs {
				results[i] = nodes[i]

				progressMu.Lock()
				statuses[workerID] = workerStatus{node: results[i], detail: "starting…", active: true}
				render()
				progressMu.Unlock()

				emit := func(node sub.Node, detail string) {
					progressMu.Lock()
					statuses[workerID].node = node
					statuses[workerID].detail = cleanLiveText(detail)
					render()
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
				statuses[workerID].node = results[i]
				statuses[workerID].detail = stageResultDetail(label, results[i])
				statuses[workerID].active = false
				render()
				progressMu.Unlock()
			}
		}(worker)
	}
	for i := range nodes {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	if terminalWidth <= 0 {
		fmt.Fprintln(stdout)
	}
	return results, callbackErr
}
func formatLiveWorkerStatusLine(label string, worker int, node sub.Node, detail string, terminalWidth int) string {
	head := fmt.Sprintf("[%s worker %d]", label, worker)
	return formatLiveStatusParts(head, liveNodeTitle(node), detail, livePingText(node), terminalWidth)
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
		detail := "URL: PASS"
		if n.RenameError != "" {
			detail += " | rename: " + shortLiveError(n.RenameError)
		}
		return detail
	}
	if n.URLError != "" {
		return "URL: FAIL (" + shortLiveError(n.URLError) + ")"
	}
	return "URL: FAIL"
}

func speedLiveDetail(n sub.Node, state string) string {
	return fmt.Sprintf("download: %.0f Mbps | upload: %.0f Mbps | %s", n.DownloadMbps, n.UploadMbps, cleanLiveText(state))
}

func speedProgressDetail(n sub.Node, progress check.SpeedProgress) string {
	state := progress.Phase
	downloadMbps, uploadMbps := progress.DownloadMbps, progress.UploadMbps
	if progress.Phase == "download" {
		if progress.TotalBytes > 0 {
			state = fmt.Sprintf("downloading %d/%d bytes", progress.CompletedBytes, progress.TotalBytes)
		} else {
			state = fmt.Sprintf("downloading %d bytes", progress.CompletedBytes)
		}
		if uploadMbps == 0 {
			uploadMbps = n.UploadMbps
		}
	} else if progress.Phase == "upload" {
		if progress.TotalBytes > 0 {
			state = fmt.Sprintf("uploading %d/%d bytes", progress.CompletedBytes, progress.TotalBytes)
		} else {
			state = fmt.Sprintf("uploading %d bytes", progress.CompletedBytes)
		}
		if downloadMbps == 0 {
			downloadMbps = n.DownloadMbps
		}
	}
	return fmt.Sprintf("download: %.0f Mbps | upload: %.0f Mbps | %s", downloadMbps, uploadMbps, state)
}

// formatLiveStatusLine keeps ping as a fixed right-hand field. The progress
// counters remain first; the config name and stage details share the remaining
// width and are shortened as needed. One terminal column is reserved to avoid
// accidental auto-wrap at the right margin.
func formatLiveStatusLine(label string, done, total, passed int, node sub.Node, detail string, terminalWidth int) string {
	head := fmt.Sprintf("[%s] %d/%d | passed: %d | failed: %d", label, done, total, passed, done-passed)
	return formatLiveStatusParts(head, liveNodeTitle(node), detail, livePingText(node), terminalWidth)
}

func formatLiveStatusParts(head, identity, detail, ping string, terminalWidth int) string {
	identity = cleanLiveText(identity)
	detail = cleanLiveText(detail)

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
	case "CLOUDFLARE FALLBACK":
		state := "FAIL"
		if n.CloudflarePassed {
			state = "PASS (Cloudflare)"
		} else if n.CloudflareError != "" {
			state += ": " + shortLiveError(n.CloudflareError)
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
		reasons = append(reasons, fmt.Sprintf("download %.0f < %.0f Mbps", download, minDownload))
	}
	if upload < minUpload {
		reasons = append(reasons, fmt.Sprintf("upload %.0f < %.0f Mbps", upload, minUpload))
	}
	return strings.Join(reasons, "; ")
}

// needsCloudflareFallback selects only URL-passed nodes whose primary Ookla
// test did not pass, whether it failed technically or missed a speed threshold.
func needsCloudflareFallback(n sub.Node, enabled bool) bool {
	return enabled && n.URLPassed && n.OoklaTested && !n.OoklaPassed
}

// recordSpeedProviderResult stores provider-specific measurements independently
// and updates the generic speed result to reflect the provider that ran last.
func recordSpeedProviderResult(n *sub.Node, provider string, download, upload float64, testErr error, opts Options) {
	if n == nil {
		return
	}
	reason := ""
	if testErr != nil {
		reason = testErr.Error()
	} else {
		reason = speedThresholdError(download, upload, opts.MinDownload, opts.MinUpload)
	}
	passed := reason == ""
	n.SpeedResultProvider = provider
	n.DownloadMbps, n.UploadMbps = download, upload
	n.SpeedPassed = passed

	switch provider {
	case "ookla":
		n.OoklaTested = true
		n.OoklaPassed = passed
		n.OoklaDownloadMbps, n.OoklaUploadMbps = download, upload
		n.OoklaError = reason
	case "cloudflare":
		n.CloudflareTested = true
		n.CloudflarePassed = passed
		n.CloudflareDownloadMbps, n.CloudflareUploadMbps = download, upload
		n.CloudflareError = reason
	}
	n.SpeedError = reason
	if provider == "cloudflare" && n.OoklaTested && !n.OoklaPassed {
		if passed {
			n.SpeedError = ""
		} else {
			n.SpeedError = "Ookla: " + n.OoklaError + "; Cloudflare: " + n.CloudflareError
		}
	}
}

func runSpeedTest(parent context.Context, n sub.Node, opts Options) (float64, float64, error) {
	return runSpeedTestWithProgress(parent, n, opts, nil)
}

func runSpeedTestWithProgress(parent context.Context, n sub.Node, opts Options, progress func(check.SpeedProgress)) (float64, float64, error) {
	return runSpeedTestWithProviderProgress(parent, n, opts, opts.SpeedProvider, progress)
}

func runCloudflareSpeedTestWithProgress(parent context.Context, n sub.Node, opts Options, progress func(check.SpeedProgress)) (float64, float64, error) {
	return runSpeedTestWithProviderProgress(parent, n, opts, "cloudflare", progress)
}

func runSpeedTestWithProviderProgress(parent context.Context, n sub.Node, opts Options, provider string, progress func(check.SpeedProgress)) (float64, float64, error) {
	var down, up float64
	err := withNodeClient(parent, n, opts, opts.SpeedTimeout, func(ctx context.Context, client *http.Client) error {
		testCtx, cancel := context.WithTimeout(ctx, opts.SpeedTimeout)
		defer cancel()
		var testErr error
		if provider == "ookla" {
			down, up, testErr = check.OoklaSpeedTestWithProgress(testCtx, client, opts.MaxSpeedDuration, progress)
		} else {
			down, up, testErr = check.SpeedTestWithProgress(testCtx, client, opts.SpeedDownloadURL, opts.SpeedUploadURL, opts.DownloadBytes, opts.UploadBytes, progress, opts.FirstByteTimeout, opts.MaxSpeedDuration, opts.SpeedSampleInterval)
		}
		return testErr
	})
	return down, up, err
}


func gamingQualityDecision(q check.NetworkQuality, limits GamingQualityOptions) (bool, string) {
	var failures []string
	if q.LatencyMS > limits.MaxLatencyMS {
		failures = append(failures, fmt.Sprintf("latency %.1f ms > %.1f ms", q.LatencyMS, limits.MaxLatencyMS))
	}
	if q.JitterMS > limits.MaxJitterMS {
		failures = append(failures, fmt.Sprintf("jitter %.1f ms > %.1f ms", q.JitterMS, limits.MaxJitterMS))
	}
	if q.DownloadLoadedLatencyMS > limits.MaxLoadedLatencyMS {
		failures = append(failures, fmt.Sprintf("download-loaded latency %.1f ms > %.1f ms", q.DownloadLoadedLatencyMS, limits.MaxLoadedLatencyMS))
	}
	if q.UploadLoadedLatencyMS > limits.MaxLoadedLatencyMS {
		failures = append(failures, fmt.Sprintf("upload-loaded latency %.1f ms > %.1f ms", q.UploadLoadedLatencyMS, limits.MaxLoadedLatencyMS))
	}
	if q.DownloadLoadedJitterMS > limits.MaxLoadedJitterMS {
		failures = append(failures, fmt.Sprintf("download-loaded jitter %.1f ms > %.1f ms", q.DownloadLoadedJitterMS, limits.MaxLoadedJitterMS))
	}
	if q.UploadLoadedJitterMS > limits.MaxLoadedJitterMS {
		failures = append(failures, fmt.Sprintf("upload-loaded jitter %.1f ms > %.1f ms", q.UploadLoadedJitterMS, limits.MaxLoadedJitterMS))
	}
	return len(failures) == 0, strings.Join(failures, "; ")
}

func gamingQualityLiveDetail(q check.NetworkQuality, passed bool, reason string) string {
	status := "CANDIDATE 🎮"
	if !passed {
		status = "NO MARK"
	}
	detail := fmt.Sprintf(
		"Gaming %s | idle %.1f/%.1f ms | loaded↓ %.1f/%.1f ms | loaded↑ %.1f/%.1f ms | packet loss: n/a",
		status, q.LatencyMS, q.JitterMS,
		q.DownloadLoadedLatencyMS, q.DownloadLoadedJitterMS,
		q.UploadLoadedLatencyMS, q.UploadLoadedJitterMS,
	)
	if reason != "" {
		detail += " | " + reason
	}
	return detail
}

func writeGamingQualityReport(dir string, nodes []sub.Node) error {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	if err := writer.Write([]string{
		"name", "protocol", "tested", "gaming_candidate",
		"latency_ms", "jitter_ms",
		"download_loaded_latency_ms", "download_loaded_jitter_ms",
		"upload_loaded_latency_ms", "upload_loaded_jitter_ms",
		"packet_loss", "result",
	}); err != nil {
		return err
	}
	for _, n := range nodes {
		row := []string{
			n.Name, protocolOf(n),
			strconv.FormatBool(n.GamingTested),
			strconv.FormatBool(n.GamingPassed),
			strconv.FormatFloat(n.GamingLatencyMS, 'f', 2, 64),
			strconv.FormatFloat(n.GamingJitterMS, 'f', 2, 64),
			strconv.FormatFloat(n.GamingDownloadLoadedLatencyMS, 'f', 2, 64),
			strconv.FormatFloat(n.GamingDownloadLoadedJitterMS, 'f', 2, 64),
			strconv.FormatFloat(n.GamingUploadLoadedLatencyMS, 'f', 2, 64),
			strconv.FormatFloat(n.GamingUploadLoadedJitterMS, 'f', 2, 64),
			"not_measured", n.GamingError,
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, "gaming-quality.csv"), buffer.Bytes())
}

func urlStageNodeName(country string) string {
	return urlStageNodeNameWithFormat(country, "full")
}

func urlStageNodeNameWithFormat(country, format string) string {
	flag, name := countryLabelForFormat(country, format)
	return strings.TrimSpace(flag + " " + name)
}

func countryLabelForFormat(country, format string) (string, string) {
	flag, fullName := countryLabel(country)
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "none":
		return flag, ""
	case "code":
		if flag == "🌐" {
			return flag, fullName
		}
		return flag, strings.ToUpper(strings.TrimSpace(country))
	default:
		return flag, fullName
	}
}

// countryLabel converts a two-letter ISO code into a stable flag and English
// country name. Unknown locations are still renamed instead of preserving a
// stale subscription remark.
func countryLabel(country string) (string, string) {
	code := strings.ToUpper(strings.TrimSpace(country))
	flag := countryFlag(code)
	if flag == "" {
		return "🌐", "Unknown"
	}
	region, err := language.ParseRegion(code)
	if err != nil {
		return flag, "Unknown"
	}
	name := strings.TrimSpace(display.English.Regions().Name(region))
	if name == "" || strings.EqualFold(name, code) {
		name = "Unknown"
	}
	return flag, name
}

func ensureCountryPlaceholder(template string) string {
	if strings.Contains(template, "{country}") {
		return template
	}
	if strings.Contains(template, "{flag}") {
		return strings.Replace(template, "{flag}", "{flag} {country}", 1)
	}
	return "{country} " + template
}

func renderNodeName(template, country string, download, upload float64) string {
	return renderNodeNameWithCountryFormat(template, country, download, upload, "full")
}

func renderNodeNameWithCountryFormat(template, country string, download, upload float64, format string) string {
	flag, countryName := countryLabelForFormat(country, format)
	if strings.ToLower(strings.TrimSpace(format)) != "none" {
		template = ensureCountryPlaceholder(template)
	}
	name := strings.NewReplacer(
		"{flag}", flag,
		"{country}", countryName,
		"{download}", formatMbps(download, 0),
		"{upload}", formatMbps(upload, 0),
	).Replace(template)
	return strings.Join(strings.Fields(name), " ")
}

func renameNodeAfterURL(parent context.Context, n *sub.Node, opts Options) string {
	if n == nil {
		return "cannot rename a nil node"
	}
	countryErr := ensureNodeCountry(parent, n, opts)
	name := urlStageNodeNameWithFormat(n.Country, opts.CountryFormat)
	if err := sub.RenameNode(n, name); err != nil {
		return err.Error()
	}
	if countryErr != nil {
		return countryErr.Error() + "; renamed with Unknown country"
	}
	return ""
}

func renameNodeAfterSpeed(parent context.Context, n *sub.Node, opts Options, download, upload float64) string {
	if n == nil {
		return "cannot rename a nil node"
	}
	countryErr := ensureNodeCountry(parent, n, opts)
	template := opts.NameFormat
	if template == "" {
		template = DefaultOptions().NameFormat
	}
	name := renderNodeNameWithCountryFormat(template, n.Country, download, upload, opts.CountryFormat)
	if strings.TrimSpace(name) == "" {
		return "service_check.name_format produced an empty node name"
	}
	if err := sub.RenameNode(n, name); err != nil {
		return err.Error()
	}
	if countryErr != nil {
		return countryErr.Error() + "; renamed with Unknown country"
	}
	return ""
}

func ensureNodeCountry(parent context.Context, n *sub.Node, opts Options) error {
	if n == nil {
		return fmt.Errorf("node is nil")
	}
	if countryFlag(strings.ToUpper(strings.TrimSpace(n.Country))) != "" {
		n.Country = strings.ToUpper(strings.TrimSpace(n.Country))
		return nil
	}
	return withNodeClient(parent, *n, opts, opts.ServiceTimeout, func(ctx context.Context, client *http.Client) error {
		// Bound each GeoIP provider independently. A stalled first provider
		// must not consume the entire service timeout and prevent fallbacks.
		geoCtx, cancelGeo := context.WithTimeout(ctx, 10*time.Second)
		country := lookupCountry(geoCtx, client, opts.GeoIPURL)
		cancelGeo()
		if country == "" {
			fallbackCtx, cancelFallback := context.WithTimeout(ctx, 6*time.Second)
			country = check.LookupIP(fallbackCtx, client).Country
			cancelFallback()
		}
		country = strings.ToUpper(strings.TrimSpace(country))
		if countryFlag(country) == "" {
			return fmt.Errorf("GeoIP country lookup failed")
		}
		n.Country = country
		return nil
	})
}

func lookupCountry(ctx context.Context, client *http.Client, endpoint string) string {
	if client == nil {
		return ""
	}
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "https://ipwho.is/"
	}
	endpoints := []string{endpoint, "https://ipapi.co/json/", "https://api.country.is/"}
	seen := make(map[string]bool, len(endpoints))
	for _, target := range endpoints {
		target = strings.TrimSpace(target)
		if target == "" || seen[target] {
			continue
		}
		seen[target] = true
		reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, target, nil)
		if err != nil {
			cancel()
			continue
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "SubWatch/0.4 GeoIP")
		resp, err := client.Do(req)
		if err == nil {
			var data map[string]any
			decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&data)
			_ = resp.Body.Close()
			cancel()
			if decodeErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				if code := countryCodeFromResponse(data); code != "" {
					return code
				}
			}
		} else {
			cancel()
		}
		if ctx.Err() != nil {
			return ""
		}
	}
	return ""
}

func countryCodeFromResponse(data map[string]any) string {
	for _, key := range []string{"country_code", "countryCode", "country"} {
		if value, ok := data[key].(string); ok {
			value = strings.ToUpper(strings.TrimSpace(value))
			if countryFlag(value) != "" {
				return value
			}
		}
	}
	if location, ok := data["location"].(map[string]any); ok {
		for _, key := range []string{"country_code", "countryCode", "country"} {
			if value, ok := location[key].(string); ok {
				value = strings.ToUpper(strings.TrimSpace(value))
				if countryFlag(value) != "" {
					return value
				}
			}
		}
	}
	return ""
}

func countryFlag(code string) string {
	if len(code) != 2 || code[0] < 65 || code[0] > 90 || code[1] < 65 || code[1] > 90 { return "" }
	return string([]rune{rune(0x1F1E6 + int(code[0]) - 65), rune(0x1F1E6 + int(code[1]) - 65)})
}

func formatMbps(value float64, _ int) string {
	// Round Mbps to the nearest whole number for names and summaries.
	return strconv.FormatInt(int64(math.Round(value)), 10)
}

func runServiceCheck(parent context.Context, n sub.Node, opts Options) (string, string, string, check.IPDetails, error) {
	var status, note, region string
	var info check.IPDetails
	err := withNodeProxy(parent, n, opts, opts.ServiceTimeout, func(ctx context.Context, proxyURL string, dialer *net.Dialer) error {
		client, clientErr := check.NewGeminiClient(proxyURL, opts.ServiceTimeout, dialer)
		if clientErr != nil {
			return clientErr
		}
		defer check.CloseClient(client)

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
	// Proxy startup has its own readiness deadline in core.Start. The configured
	// stage timeout applies only to the actual HTTP test, not to local process
	// startup; otherwise slow startup silently shortens the network timeout.
	inst, err := core.Start(parent, opts.CorePath, n.Outbound, opts.Interface, opts.NetworkNamespace, opts.NetworkNamespaceInterface)
	if err != nil {
		return fmt.Errorf("start proxy: %w", err)
	}
	defer inst.Close()
	client, err := check.NewClient(inst.ProxyURL, timeout, inst.Dialer)
	if err != nil {
		return err
	}
	defer check.CloseClient(client)
	return fn(parent, client)
}

// withNodeProxy exposes the local sing-box HTTP CONNECT endpoint so Gemini can
// use Throne-compatible TLS/HTTP2 without changing the other test stages.
func withNodeProxy(parent context.Context, n sub.Node, opts Options, timeout time.Duration, fn func(context.Context, string, *net.Dialer) error) error {
	inst, err := core.Start(parent, opts.CorePath, n.Outbound, opts.Interface, opts.NetworkNamespace, opts.NetworkNamespaceInterface)
	if err != nil {
		return fmt.Errorf("start proxy: %w", err)
	}
	defer inst.Close()
	return fn(parent, inst.ProxyURL, inst.Dialer)
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
		"all.txt", "failed.txt", "gaming-quality.txt", "gaming-quality.csv", "invalid.txt", "unique.txt", "selected.txt", "url-tested.txt", "speedtested.txt",
		"url-test-finished.txt", "url-test-failed.txt", "url-test-failures.txt",
		"speed-test-finished.txt", "speed-test-report.csv", "cloudflare-fallback-finished.txt", "speedtest-finished.txt", "speedtest-failed.txt", "tested.txt", "service-check-results.txt",
		"final.txt", "gemini-available.txt", "gemini.txt", "report.csv",
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

const geminiNameSuffix = " Gemini 🐱"

// writeGeminiResults optionally supports the legacy separate-file mode.
// The current Run pipeline always uses inline name markers to keep output compact.
func writeGeminiResults(dir string, geminiNodes, allNodes, urlNodes, speedNodes []sub.Node, separateFile bool) error {
	if err := markGeminiNodes(geminiNodes); err != nil {
		return err
	}
	if separateFile {
		// Keep the dedicated Gemini list's URI remarks consistent with the
		// marked names used in the combined lists.
		return writeStageFiles(dir, "gemini", geminiNodes, true)
	}
	if err := os.Remove(filepath.Join(dir, "gemini.txt")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := clearProtocolSubscriptions(dir, "gemini"); err != nil {
		return err
	}

	for _, group := range [][]sub.Node{allNodes, urlNodes, speedNodes} {
		applyNodeUpdatesByIdentity(group, geminiNodes)
	}
	if err := writeSubscription(filepath.Join(dir, "all.txt"), allNodes); err != nil {
		return err
	}
	if err := writeStageFiles(dir, "url-tested", urlNodes, true); err != nil {
		return err
	}
	return writeStageFiles(dir, "speedtested", speedNodes, true)
}

func markGeminiNodes(nodes []sub.Node) error {
	for i := range nodes {
		if !geminiAvailable(nodes[i]) {
			continue
		}
		name := strings.TrimSpace(nodes[i].Name)
		if strings.HasSuffix(name, geminiNameSuffix) {
			continue
		}
		if name == "" {
			name = nodes[i].Protocol
		}
		if err := sub.RenameNode(&nodes[i], name+geminiNameSuffix); err != nil {
			return fmt.Errorf("rename Gemini node %s: %w", nodes[i].Protocol, err)
		}
	}
	return nil
}

const gamingNameSuffix = " 🎮"

// qualityMarkedName keeps quality markers mutually exclusive. A gaming marker
// always takes precedence over the less-specific stability marker.
func qualityMarkedName(name string, stable, gaming bool) string {
	name = strings.TrimSpace(name)
	hasGemini := strings.HasSuffix(name, geminiNameSuffix)
	if hasGemini {
		name = strings.TrimSpace(strings.TrimSuffix(name, geminiNameSuffix))
	}
	// Always strip old quality suffixes first. The current cycle's measured
	// quality decides which marker remains; stale 🎮/🧱 labels never persist.
	for {
		switch {
		case strings.HasSuffix(name, gamingNameSuffix):
			name = strings.TrimSpace(strings.TrimSuffix(name, gamingNameSuffix))
		case strings.HasSuffix(name, stabilityNameSuffix):
			name = strings.TrimSpace(strings.TrimSuffix(name, stabilityNameSuffix))
		default:
			if gaming {
				name += gamingNameSuffix
			} else if stable {
				name += stabilityNameSuffix
			}
			if hasGemini {
				name += geminiNameSuffix
			}
			return name
		}
	}
}

func applyStabilityMarkers(nodes []sub.Node, streaks map[string]int, gaming map[string]bool) error {
	for i := range nodes {
		key := nodeIdentity(nodes[i])
		stable := streaks[key] >= stableConsecutiveCycles
		name := qualityMarkedName(nodes[i].Name, stable, gaming[key])
		if name == nodes[i].Name {
			continue
		}
		if err := sub.RenameNode(&nodes[i], name); err != nil {
			return fmt.Errorf("rename quality marker for node %s: %w", nodes[i].Protocol, err)
		}
	}
	return nil
}

func writeStabilityResults(dir string, allNodes, urlNodes, speedNodes, geminiNodes []sub.Node, streaks map[string]int, gaming map[string]bool) error {
	for _, group := range [][]sub.Node{allNodes, urlNodes, speedNodes, geminiNodes} {
		if err := applyStabilityMarkers(group, streaks, gaming); err != nil {
			return err
		}
	}
	if err := writeSubscription(filepath.Join(dir, "all.txt"), allNodes); err != nil {
		return err
	}
	if err := writeStageFiles(dir, "url-tested", urlNodes, true); err != nil {
		return err
	}
	return writeStageFiles(dir, "speedtested", speedNodes, true)
}

func nodeIdentity(n sub.Node) string {
	if n.Key != "" {
		return n.Key
	}
	return n.URI
}

func applyNodeUpdatesByIdentity(nodes, updates []sub.Node) {
	byIdentity := make(map[string]sub.Node, len(updates))
	for _, n := range updates {
		byIdentity[nodeIdentity(n)] = n
	}
	for i := range nodes {
		if updated, ok := byIdentity[nodeIdentity(nodes[i])]; ok {
			nodes[i].URI = updated.URI
			nodes[i].Name = updated.Name
		}
	}
}

// removeLegacyJSONArtifacts clears output snapshots from versions that exported
// Node structures as JSON. It only touches known SubWatch-generated names.
func removeLegacyJSONArtifacts(dir string) error {
	bases := []string{
		"all", "unique", "selected",
		"url-tested", "speedtested",
		"url-test-finished", "url-test-failed",
		"speed-test-finished", "speedtest-failed", "tested",
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
	// Subscription files are exported and uploaded for HTTP serving. Keep their
	// contents readable by the web server while the private output directory
	// itself remains restricted to the SubWatch user.
	if err := tmp.Chmod(0644); err != nil {
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

// writeURLFailureReport creates a human-readable report with the exact reason
// each node failed. The separate url-test-failed.txt remains a URI-only list
// that can be imported into another proxy client.
func writeURLFailureReport(path, target string, nodes []sub.Node) error {
	var b strings.Builder
	fmt.Fprintln(&b, "SubWatch URL TEST failures")
	fmt.Fprintf(&b, "Target: %s\n", cleanLiveText(target))
	fmt.Fprintf(&b, "Failed nodes: %d\n", len(nodes))
	if len(nodes) == 0 {
		fmt.Fprintln(&b, "\nNo URL test failures.")
		return writeAtomic(path, []byte(b.String()))
	}
	for i, n := range nodes {
		name := cleanLiveText(n.Name)
		if name == "" {
			name = "unnamed config"
		}
		reason := cleanLiveText(n.URLError)
		if reason == "" {
			reason = "URL test failed without an error message"
		}
		fmt.Fprintf(&b, "\n[%d] %s (%s)\n", i+1, name, protocolOf(n))
		fmt.Fprintf(&b, "Latency: %d ms\n", n.URLLatencyMS)
		fmt.Fprintf(&b, "Reason: %s\n", reason)
		fmt.Fprintf(&b, "URI: %s\n", cleanLiveText(n.URI))
	}
	return writeAtomic(path, []byte(b.String()))
}

func writeSubscription(path string, nodes []sub.Node) error {
	var b strings.Builder
	for _, n := range nodes {
		b.WriteString(n.URI)
		b.WriteByte('\n')
	}
	return writeAtomic(path, []byte(b.String()))
}

// writeInvalidLines keeps rejected proxy entries as plain input lines.
func writeInvalidLines(path string, lines []string) error {
	var b strings.Builder
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return writeAtomic(path, []byte(b.String()))
}

func writeSpeedTestReport(path string, nodes []sub.Node) error {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	if err := writer.Write([]string{
		"name", "protocol", "uri", "url_passed", "url_latency_ms",
		"ookla_tested", "ookla_passed", "ookla_download_mbps", "ookla_upload_mbps", "ookla_error",
		"cloudflare_tested", "cloudflare_passed", "cloudflare_download_mbps", "cloudflare_upload_mbps", "cloudflare_error",
		"speed_passed", "speed_result_provider", "download_mbps", "upload_mbps", "speed_error",
	}); err != nil {
		return err
	}
	for _, n := range nodes {
		if err := writer.Write([]string{
			n.Name, protocolOf(n), n.URI, strconv.FormatBool(n.URLPassed), strconv.FormatInt(n.URLLatencyMS, 10),
			strconv.FormatBool(n.OoklaTested), strconv.FormatBool(n.OoklaPassed), fmt.Sprintf("%.2f", n.OoklaDownloadMbps), fmt.Sprintf("%.2f", n.OoklaUploadMbps), n.OoklaError,
			strconv.FormatBool(n.CloudflareTested), strconv.FormatBool(n.CloudflarePassed), fmt.Sprintf("%.2f", n.CloudflareDownloadMbps), fmt.Sprintf("%.2f", n.CloudflareUploadMbps), n.CloudflareError,
			strconv.FormatBool(n.SpeedPassed), n.SpeedResultProvider, fmt.Sprintf("%.2f", n.DownloadMbps), fmt.Sprintf("%.2f", n.UploadMbps), n.SpeedError,
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	return writeAtomic(path, buffer.Bytes())
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
