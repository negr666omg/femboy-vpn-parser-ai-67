package check

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ooklaServerListURL          = "https://www.speedtest.net/api/js/servers"
	ooklaServerCandidateLimit   = 12
	ooklaProbeTimeout           = 2 * time.Second
	ooklaDirectionMaxDuration   = 8 * time.Second
	ooklaPeakWindow             = 2 * time.Second
	ooklaProgressInterval       = 500 * time.Millisecond
	ooklaWorkerCount            = 4
	ooklaPayloadSize            = 1 << 20
	ooklaMaxConsecutiveFailures = 3
	ooklaRetryDelay             = 150 * time.Millisecond
)

type ooklaServer struct {
	URL     string `json:"url"`
	Name    string `json:"name"`
	Country string `json:"country"`
	Sponsor string `json:"sponsor"`
	ID      string `json:"id"`
}

type ooklaServerProbe struct {
	server  ooklaServer
	latency time.Duration
}

type ooklaCountingReader struct {
	reader io.Reader
	count  atomic.Int64
	onRead func(int64)
}

func (r *ooklaCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.count.Add(int64(n))
		if r.onRead != nil {
			r.onRead(int64(n))
		}
	}
	return n, err
}

type ooklaRateSample struct {
	at    time.Time
	bytes int64
}

type ooklaRateSampler struct {
	samples []ooklaRateSample
	peak    float64
}

func newOoklaRateSampler(start time.Time) *ooklaRateSampler {
	return &ooklaRateSampler{samples: []ooklaRateSample{{at: start}}}
}

// observe returns a rolling throughput average over approximately the last
// two seconds. Keeping this window independent of individual HTTP response
// boundaries avoids the 0 -> burst -> 0 pattern caused by counting only when
// a 1 MiB request has completely finished.
func (s *ooklaRateSampler) observe(now time.Time, totalBytes int64) float64 {
	s.samples = append(s.samples, ooklaRateSample{at: now, bytes: totalBytes})
	cutoff := now.Add(-ooklaPeakWindow)

	baseIndex := 0
	for i := 1; i < len(s.samples); i++ {
		if s.samples[i].at.After(cutoff) {
			break
		}
		baseIndex = i
	}
	base := s.samples[baseIndex]
	elapsed := now.Sub(base.at).Seconds()
	rate := 0.0
	if elapsed > 0 && totalBytes >= base.bytes {
		rate = float64(totalBytes-base.bytes) * 8 / elapsed / 1e6
	}
	// Only consider a near-full rolling window for the final peak. Short
	// startup bursts are deliberately excluded from the pass/fail result.
	if now.Sub(base.at) >= ooklaPeakWindow-ooklaProgressInterval && rate > s.peak {
		s.peak = rate
	}
	if baseIndex > 0 {
		samples := append([]ooklaRateSample(nil), s.samples[baseIndex:]...)
		s.samples = samples
	}
	return rate
}

func (s *ooklaRateSampler) result(now, start time.Time, totalBytes int64) float64 {
	s.observe(now, totalBytes)
	if s.peak > 0 {
		return s.peak
	}
	elapsed := now.Sub(start).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(totalBytes) * 8 / elapsed / 1e6
}

// OoklaSpeedTestWithProgress discovers Speedtest.net servers automatically,
// selects the lowest-latency reachable server through the supplied per-node
// proxy, and measures download/upload throughput against that server. The
// result is the maximum sustained two-second rate, not the whole-test average.
func OoklaSpeedTestWithProgress(ctx context.Context, client *http.Client, duration time.Duration, progress func(SpeedProgress)) (float64, float64, error) {
	return ooklaSpeedTestWithEndpoint(ctx, client, ooklaServerListURL, duration, progress)
}

// ooklaSpeedTestWithEndpoint is split out so server discovery can be tested
// against a local Speedtest.net-compatible endpoint without contacting the
// public service.
func ooklaSpeedTestWithEndpoint(ctx context.Context, client *http.Client, endpoint string, duration time.Duration, progress func(SpeedProgress)) (float64, float64, error) {
	if client == nil {
		return 0, 0, fmt.Errorf("Ookla speed test requires an HTTP client")
	}
	if duration <= 0 {
		duration = ooklaDirectionMaxDuration
	}
	if duration > ooklaDirectionMaxDuration {
		duration = ooklaDirectionMaxDuration
	}

	listCtx, cancelList := context.WithTimeout(ctx, 5*time.Second)
	servers, err := fetchOoklaServers(listCtx, client, endpoint)
	cancelList()
	if err != nil {
		return 0, 0, err
	}

	probeCtx, cancelProbe := context.WithTimeout(ctx, ooklaProbeTimeout+250*time.Millisecond)
	server, err := selectOoklaServer(probeCtx, client, servers)
	cancelProbe()
	if err != nil {
		return 0, 0, err
	}

	if progress != nil {
		progress(SpeedProgress{Phase: fmt.Sprintf("Ookla server: %s, %s", server.Name, server.Sponsor)})
	}
	downloadURL, err := ooklaDownloadURL(server.URL)
	if err != nil {
		return 0, 0, err
	}

	down, err := runOoklaDirection(ctx, client, http.MethodGet, downloadURL, duration, progress, "download")
	if err != nil {
		return 0, 0, fmt.Errorf("Ookla download: %w", err)
	}
	uploadURL := resolveOoklaUploadURL(ctx, client, server.URL)
	up, err := runOoklaDirection(ctx, client, http.MethodPost, uploadURL, duration, progress, "upload")
	if err != nil {
		return down, 0, fmt.Errorf("Ookla upload: %w", err)
	}
	return down, up, nil
}

func fetchOoklaServers(ctx context.Context, client *http.Client, endpoint string) ([]ooklaServer, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid Ookla server-list endpoint")
	}
	query := u.Query()
	query.Set("engine", "js")
	query.Set("https_functional", "true")
	query.Set("limit", strconv.Itoa(ooklaServerCandidateLimit))
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create Ookla server-list request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; SubWatch/0.4; speedtest)")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch Ookla server list: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Ookla server list returned HTTP %d", resp.StatusCode)
	}

	var servers []ooklaServer
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&servers); err != nil {
		return nil, fmt.Errorf("decode Ookla server list: %w", err)
	}
	if len(servers) == 0 {
		return nil, fmt.Errorf("Ookla returned an empty server list")
	}
	if len(servers) > ooklaServerCandidateLimit {
		servers = servers[:ooklaServerCandidateLimit]
	}
	return servers, nil
}

func selectOoklaServer(ctx context.Context, client *http.Client, servers []ooklaServer) (ooklaServer, error) {
	type result struct {
		probe ooklaServerProbe
		err   error
	}
	results := make(chan result, len(servers))
	var workers sync.WaitGroup

	for _, server := range servers {
		server := server
		if _, err := ooklaLatencyURL(server.URL); err != nil {
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			probeCtx, cancel := context.WithTimeout(ctx, ooklaProbeTimeout)
			defer cancel()

			target, _ := ooklaLatencyURL(server.URL)
			target = addOoklaCacheBuster(target, time.Now().UnixNano())
			req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, target, nil)
			if err != nil {
				results <- result{err: err}
				return
			}
			req.Header.Set("Cache-Control", "no-cache")
			start := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				results <- result{err: err}
				return
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				results <- result{err: fmt.Errorf("latency probe returned HTTP %d", resp.StatusCode)}
				return
			}
			results <- result{probe: ooklaServerProbe{server: server, latency: time.Since(start)}}
		}()
	}
	go func() {
		workers.Wait()
		close(results)
	}()

	var best ooklaServerProbe
	found := false
	for item := range results {
		if item.err != nil {
			continue
		}
		if !found || item.probe.latency < best.latency {
			best, found = item.probe, true
		}
	}
	if err := ctx.Err(); err != nil {
		return ooklaServer{}, err
	}
	if !found {
		return ooklaServer{}, fmt.Errorf("no reachable Ookla Speedtest.net servers through this proxy")
	}
	return best.server, nil
}

func ooklaLatencyURL(uploadURL string) (string, error) {
	u, err := url.Parse(uploadURL)
	if err != nil {
		return "", err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid Ookla server URL")
	}
	u.Path = path.Join(path.Dir(u.Path), "latency.txt")
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func ooklaDownloadURL(uploadURL string) (string, error) {
	u, err := url.Parse(uploadURL)
	if err != nil {
		return "", err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid Ookla server URL")
	}
	u.Path = path.Join(path.Dir(u.Path), "random1000x1000.jpg")
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func addOoklaCacheBuster(raw string, value int64) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	query := u.Query()
	query.Set("x", strconv.FormatInt(value, 10))
	u.RawQuery = query.Encode()
	return u.String()
}

func resolveOoklaUploadURL(ctx context.Context, client *http.Client, target string) string {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodHead, target, nil)
	if err != nil {
		return target
	}
	resp, err := client.Do(req)
	if err != nil {
		return target
	}
	_ = resp.Body.Close()
	if resp.Request != nil && resp.Request.URL != nil {
		return resp.Request.URL.String()
	}
	return target
}

func runOoklaDirection(parent context.Context, client *http.Client, method, target string, duration time.Duration, progress func(SpeedProgress), phase string) (float64, error) {
	if duration <= 0 {
		duration = ooklaDirectionMaxDuration
	}
	if duration > ooklaDirectionMaxDuration {
		duration = ooklaDirectionMaxDuration
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	payload := bytes.Repeat([]byte{0x5a}, ooklaPayloadSize)
	var transferred atomic.Int64
	var requestID atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup

	recordFailure := func(err error, consecutive *int) bool {
		if ctx.Err() != nil {
			return true
		}
		errOnce.Do(func() { firstErr = err })
		*consecutive++
		if *consecutive >= ooklaMaxConsecutiveFailures {
			return true
		}
		timer := time.NewTimer(ooklaRetryDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return true
		case <-timer.C:
			return false
		}
	}

	start := time.Now()
	for i := 0; i < ooklaWorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consecutiveFailures := 0
			for ctx.Err() == nil {
				requestURL := target
				if method == http.MethodGet {
					requestURL = addOoklaCacheBuster(target, requestID.Add(1))
				}
				var body io.Reader
				var uploadBody *ooklaCountingReader
				if method == http.MethodPost {
					uploadBody = &ooklaCountingReader{
						reader: bytes.NewReader(payload),
						onRead: func(n int64) { transferred.Add(n) },
					}
					body = uploadBody
				}
				req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
				if err != nil {
					if recordFailure(err, &consecutiveFailures) {
						return
					}
					continue
				}
				req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; SubWatch/0.4; speedtest)")
				req.Header.Set("Cache-Control", "no-cache")
				req.Header.Set("Pragma", "no-cache")
				if method == http.MethodPost {
					req.Header.Set("Content-Type", "application/octet-stream")
					req.ContentLength = int64(len(payload))
				}

				resp, err := client.Do(req)
				if err != nil {
					if recordFailure(err, &consecutiveFailures) {
						return
					}
					continue
				}

				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					statusErr := fmt.Errorf("HTTP %d", resp.StatusCode)
					_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
					_ = resp.Body.Close()
					if recordFailure(statusErr, &consecutiveFailures) {
						return
					}
					continue
				}

				if method == http.MethodGet {
					reader := &ooklaCountingReader{
						reader: resp.Body,
						onRead: func(n int64) { transferred.Add(n) },
					}
					_, copyErr := io.Copy(io.Discard, reader)
					_ = resp.Body.Close()
					if copyErr != nil {
						if ctx.Err() != nil {
							return
						}
						if recordFailure(copyErr, &consecutiveFailures) {
							return
						}
						continue
					}
					if reader.count.Load() == 0 {
						if recordFailure(fmt.Errorf("empty download response"), &consecutiveFailures) {
							return
						}
						continue
					}
				} else {
					_, copyErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
					_ = resp.Body.Close()
					if copyErr != nil {
						if ctx.Err() != nil {
							return
						}
						if recordFailure(copyErr, &consecutiveFailures) {
							return
						}
						continue
					}
					if uploadBody.count.Load() != int64(len(payload)) {
						if recordFailure(fmt.Errorf("upload body was incomplete: %d/%d bytes", uploadBody.count.Load(), len(payload)), &consecutiveFailures) {
							return
						}
						continue
					}
				}
				consecutiveFailures = 0
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	sampler := newOoklaRateSampler(start)
	ticker := time.NewTicker(ooklaProgressInterval)
	defer ticker.Stop()
	timer := time.NewTimer(duration)
	defer timer.Stop()

	finish := func() (float64, error) {
		cancel()
		<-done
		total := transferred.Load()
		if total == 0 {
			if firstErr != nil {
				return 0, firstErr
			}
			return 0, fmt.Errorf("no data transferred")
		}
		now := time.Now()
		rate := sampler.result(now, start, total)
		if rate <= 0 {
			return 0, fmt.Errorf("could not calculate Ookla throughput")
		}
		if progress != nil {
			event := SpeedProgress{Phase: phase + " complete", CompletedBytes: total}
			if phase == "download" {
				event.DownloadMbps = rate
			} else {
				event.UploadMbps = rate
			}
			progress(event)
		}
		return rate, nil
	}

	for {
		select {
		case <-parent.Done():
			cancel()
			<-done
			return 0, parent.Err()
		case <-timer.C:
			return finish()
		case <-done:
			total := transferred.Load()
			if total == 0 {
				if firstErr != nil {
					return 0, firstErr
				}
				return 0, fmt.Errorf("no data transferred")
			}
			return finish()
		case now := <-ticker.C:
			total := transferred.Load()
			rate := sampler.observe(now, total)
			if progress != nil {
				event := SpeedProgress{Phase: phase, CompletedBytes: total}
				if phase == "download" {
					event.DownloadMbps = rate
				} else {
					event.UploadMbps = rate
				}
				progress(event)
			}
		}
	}
}
