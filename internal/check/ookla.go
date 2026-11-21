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
	"sync"
	"sync/atomic"
	"time"
)

const ooklaServerListURL = "https://www.speedtest.net/api/js/servers"

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
}

func (r *ooklaCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.count.Add(int64(n))
	return n, err
}

// OoklaSpeedTestWithProgress discovers Speedtest.net servers automatically, selects
// the fastest reachable server through the supplied per-node proxy, and measures
// download and upload throughput against that server.
func OoklaSpeedTestWithProgress(ctx context.Context, client *http.Client, duration time.Duration, progress func(SpeedProgress)) (float64, float64, error) {
	return ooklaSpeedTestWithEndpoint(ctx, client, ooklaServerListURL, duration, progress)
}

// ooklaSpeedTestWithEndpoint is split out so server discovery can be tested against
// a local Speedtest.net-compatible endpoint without contacting the public service.
func ooklaSpeedTestWithEndpoint(ctx context.Context, client *http.Client, endpoint string, duration time.Duration, progress func(SpeedProgress)) (float64, float64, error) {
	if client == nil {
		return 0, 0, fmt.Errorf("Ookla speed test requires an HTTP client")
	}
	if duration <= 0 {
		duration = 10 * time.Second
	}

	servers, err := fetchOoklaServers(ctx, client, endpoint)
	if err != nil {
		return 0, 0, err
	}
	server, err := selectOoklaServer(ctx, client, servers)
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
	query.Set("limit", "30")
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create Ookla server-list request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; SubWatch/0.4; speedtest)")
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
	if len(servers) > 30 {
		servers = servers[:30]
	}
	return servers, nil
}

func selectOoklaServer(ctx context.Context, client *http.Client, servers []ooklaServer) (ooklaServer, error) {
	type result struct {
		probe ooklaServerProbe
		err   error
	}
	results := make(chan result, len(servers))
	limit := make(chan struct{}, 8)
	var workers sync.WaitGroup

	for _, server := range servers {
		server := server
		if _, err := ooklaLatencyURL(server.URL); err != nil {
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			select {
			case limit <- struct{}{}:
			case <-ctx.Done():
				results <- result{err: ctx.Err()}
				return
			}
			defer func() { <-limit }()

			probeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			defer cancel()
			target, _ := ooklaLatencyURL(server.URL)
			req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, target, nil)
			if err != nil {
				results <- result{err: err}
				return
			}
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

func resolveOoklaUploadURL(ctx context.Context, client *http.Client, target string) string {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	const workers = 4
	payload := bytes.Repeat([]byte{0x5a}, 1<<20)
	var transferred atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				var body io.Reader
				var uploadBody *ooklaCountingReader
				if method == http.MethodPost {
					uploadBody = &ooklaCountingReader{reader: bytes.NewReader(payload)}
					body = uploadBody
				}
				req, err := http.NewRequestWithContext(ctx, method, target, body)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; SubWatch/0.4; speedtest)")
				if method == http.MethodPost {
					req.Header.Set("Content-Type", "application/octet-stream")
					req.ContentLength = int64(len(payload))
				}
				resp, err := client.Do(req)
				if err != nil {
					if ctx.Err() == nil {
						errOnce.Do(func() { firstErr = err })
					}
					return
				}
				if method == http.MethodGet {
					n, copyErr := io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						errOnce.Do(func() { firstErr = fmt.Errorf("HTTP %d", resp.StatusCode) })
						return
					}
					if n > 0 {
						transferred.Add(n)
					}
					if copyErr != nil {
						if ctx.Err() == nil {
							errOnce.Do(func() { firstErr = copyErr })
						}
						return
					}
					if n == 0 {
						errOnce.Do(func() { firstErr = fmt.Errorf("empty download response") })
						return
					}
				} else {
					_, copyErr := io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						errOnce.Do(func() { firstErr = fmt.Errorf("HTTP %d", resp.StatusCode) })
						return
					}
					if copyErr != nil {
						if ctx.Err() == nil {
							errOnce.Do(func() { firstErr = copyErr })
						}
						return
					}
					if sent := uploadBody.count.Load(); sent != int64(len(payload)) {
						errOnce.Do(func() { firstErr = fmt.Errorf("upload server confirmed an incomplete body: %d/%d bytes", sent, len(payload)) })
						return
					}
					transferred.Add(int64(len(payload)))
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	lastBytes := int64(0)
	lastReport := start

	for {
		select {
		case <-parent.Done():
			cancel()
			<-done
			return 0, parent.Err()
		case <-timer.C:
			cancel()
			<-done
			elapsed := time.Since(start).Seconds()
			total := transferred.Load()
			if total == 0 {
				if firstErr != nil {
					return 0, firstErr
				}
				return 0, fmt.Errorf("no data transferred")
			}
			rate := float64(total) * 8 / elapsed / 1e6
			if progress != nil {
				event := SpeedProgress{Phase: phase, CompletedBytes: total}
				if phase == "download" {
					event.DownloadMbps = rate
				} else {
					event.UploadMbps = rate
				}
				progress(event)
			}
			return rate, nil
		case <-done:
			elapsed := time.Since(start).Seconds()
			total := transferred.Load()
			if firstErr != nil {
				return 0, firstErr
			}
			if total == 0 || elapsed <= 0 {
				return 0, fmt.Errorf("no data transferred")
			}
			rate := float64(total) * 8 / elapsed / 1e6
			if progress != nil {
				event := SpeedProgress{Phase: phase, CompletedBytes: total}
				if phase == "download" {
					event.DownloadMbps = rate
				} else {
					event.UploadMbps = rate
				}
				progress(event)
			}
			return rate, nil
		case now := <-ticker.C:
			total := transferred.Load()
			seconds := now.Sub(lastReport).Seconds()
			rate := 0.0
			if seconds > 0 {
				rate = float64(total-lastBytes) * 8 / seconds / 1e6
			}
			if progress != nil {
				event := SpeedProgress{Phase: phase, CompletedBytes: total}
				if phase == "download" {
					event.DownloadMbps = rate
				} else {
					event.UploadMbps = rate
				}
				progress(event)
			}
			lastBytes, lastReport = total, now
		}
	}
}

