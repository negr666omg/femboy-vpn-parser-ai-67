package check

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/negr666omg/subwatch/internal/sub"
)

type Settings struct {
	Timeout       time.Duration
	URL           string
	DownloadBytes int64
	UploadBytes   int64
	MinDownload   float64
	MinUpload     float64
}

func NewClient(proxyURL string, timeout time.Duration, dialers ...*net.Dialer) (*http.Client, error) {
	p, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{Proxy: http.ProxyURL(p), MaxIdleConns: 4, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second, DisableCompression: true}
	if len(dialers) > 0 && dialers[0] != nil {
		tr.DialContext = dialers[0].DialContext
	}
	return &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return http.ErrUseLastResponse
		}
		return nil
	}}, nil
}
func CloseClient(c *http.Client) {
	if c == nil {
		return
	}
	switch t := c.Transport.(type) {
	case *http.Transport:
		t.CloseIdleConnections()
	case *geminiTransport:
		t.Close()
	}
}

func URLTest(ctx context.Context, client *http.Client, target string) (bool, int64, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false, 0, err
	}
	req.Header.Set("User-Agent", "SubWatch/0.1 URL check")
	resp, err := client.Do(req)
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		return false, elapsed, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return false, elapsed, fmt.Errorf("URL test returned HTTP %s", resp.Status)
	}
	return true, elapsed, nil
}

type SpeedProgress struct {
	Phase          string
	CompletedBytes int64
	TotalBytes     int64
	DownloadMbps   float64
	UploadMbps     float64
}

func SpeedTest(ctx context.Context, client *http.Client, downloadURL, uploadURL string, downloadBytes, uploadBytes int64) (float64, float64, error) {
	return SpeedTestWithProgress(ctx, client, downloadURL, uploadURL, downloadBytes, uploadBytes, nil)
}

// SpeedTestWithProgress reports live average throughput while the payloads are
// transferred. The callback is optional and is invoked synchronously by the
// transfer goroutine.
func SpeedTestWithProgress(ctx context.Context, client *http.Client, downloadURL, uploadURL string, downloadBytes, uploadBytes int64, progress func(SpeedProgress), timing ...time.Duration) (float64, float64, error) {
	firstByteTimeout, maxDuration, sampleInterval := 10*time.Second, 30*time.Second, 500*time.Millisecond
	if len(timing) > 0 && timing[0] > 0 { firstByteTimeout = timing[0] }
	if len(timing) > 1 && timing[1] > 0 { maxDuration = timing[1] }
	if len(timing) > 2 && timing[2] > 0 { sampleInterval = timing[2] }
	if downloadBytes < 64*1024 || uploadBytes < 64*1024 {
		return 0, 0, fmt.Errorf("test payload too small (minimum 64 KiB per direction)")
	}
	if downloadBytes > 100<<20 || uploadBytes > 100<<20 {
		return 0, 0, fmt.Errorf("test payload cannot exceed 100 MiB per direction")
	}
	if downloadURL == "" || uploadURL == "" {
		return 0, 0, fmt.Errorf("download and upload test URLs must be configured")
	}
	downloadURL = strings.ReplaceAll(downloadURL, "{bytes}", fmt.Sprintf("%d", downloadBytes))
	if err := validateTestURL(downloadURL); err != nil {
		return 0, 0, fmt.Errorf("download URL: %w", err)
	}
	if err := validateTestURL(uploadURL); err != nil {
		return 0, 0, fmt.Errorf("upload URL: %w", err)
	}

	downloadCtx, downloadCancel := context.WithCancel(ctx)
	defer downloadCancel()
	dreq, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return 0, 0, err
	}
	dreq.Header.Set("Accept-Encoding", "identity")
	dreq.Header.Set("User-Agent", "SubWatch/0.3 speed test")
	start := time.Now()
	firstByte := make(chan struct{})
	go enforceTransferWindow(downloadCtx, downloadCancel, firstByte, firstByteTimeout, maxDuration)
	resp, err := client.Do(dreq)
	if err != nil {
		return 0, 0, fmt.Errorf("download: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return 0, 0, fmt.Errorf("download: HTTP %d", resp.StatusCode)
	}

	var downloaded int64
	buf := make([]byte, 64*1024)
	lastReportBytes := int64(0)
	lastReportAt := time.Now()
	startedDownload := false
	var peakDown float64
	var downloadFirstByteAt time.Time
	var readErr error
	for downloaded < downloadBytes {
		want := int64(len(buf))
		if remain := downloadBytes - downloaded; remain < want {
			want = remain
		}
		n, e := resp.Body.Read(buf[:int(want)])
		if n > 0 {
			downloaded += int64(n)
			if !startedDownload { startedDownload = true; downloadFirstByteAt = time.Now(); lastReportAt = downloadFirstByteAt; lastReportBytes = 0 }
			select { case <-firstByte: default: close(firstByte) }
			now := time.Now()
			if now.Sub(lastReportAt) >= sampleInterval || downloaded == downloadBytes {
				elapsed := now.Sub(lastReportAt).Seconds()
				rate := 0.0
				if elapsed > 0 { rate = float64(downloaded-lastReportBytes) * 8 / elapsed / 1e6 }
				if rate > peakDown { peakDown = rate }
				if progress != nil { progress(SpeedProgress{Phase: "download", CompletedBytes: downloaded, TotalBytes: downloadBytes, DownloadMbps: peakDown}) }
				lastReportBytes, lastReportAt = downloaded, now
			}
		}
		if e != nil {
			if e == io.EOF && downloaded == downloadBytes {
				break
			}
			readErr = e
			break
		}
		if n == 0 {
			readErr = io.ErrNoProgress
			break
		}
	}
	_ = resp.Body.Close()
	downloadCancel()
	elapsed := time.Since(start).Seconds()
	if readErr != nil {
		return 0, 0, fmt.Errorf("download: received %d/%d bytes: %w", downloaded, downloadBytes, readErr)
	}
	if downloaded != downloadBytes || elapsed <= 0 {
		return 0, 0, fmt.Errorf("download: incomplete transfer (%d/%d bytes)", downloaded, downloadBytes)
	}
	down := peakDown
	if down == 0 { sampleSeconds := time.Since(downloadFirstByteAt).Seconds(); if sampleSeconds <= 0 { sampleSeconds = elapsed }; down = float64(downloaded) * 8 / sampleSeconds / 1e6 }
	if progress != nil {
		progress(SpeedProgress{Phase: "download complete", CompletedBytes: downloadBytes, TotalBytes: downloadBytes, DownloadMbps: down})
	}

	payload := bytes.Repeat([]byte{0x5a}, int(uploadBytes))
	ureq, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bytes.NewReader(payload))
	if err != nil {
		return down, 0, err
	}
	ureq.Header.Set("Content-Type", "application/octet-stream")
	ureq.Header.Set("Content-Length", fmt.Sprintf("%d", uploadBytes))
	ureq.Header.Set("Accept-Encoding", "identity")
	ureq.Header.Set("User-Agent", "SubWatch/0.3 speed test")
	uploadStart := time.Now()
	uploadCtx, uploadCancel := context.WithCancel(ctx)
	defer uploadCancel()
	uploadFirstByte := make(chan struct{})
	go enforceTransferWindow(uploadCtx, uploadCancel, uploadFirstByte, firstByteTimeout, maxDuration)
	ureq = ureq.WithContext(uploadCtx)
	uploadReader := &uploadProgressReader{
		reader: bytes.NewReader(payload), total: uploadBytes, started: uploadStart, interval: sampleInterval,
		onProgress: func(done int64, _ time.Duration) {
			select { case <-uploadFirstByte: default: close(uploadFirstByte) }
			// Bytes read by net/http are not proof that bytes reached the network.
			// Keep progress byte-based and report the measured rate only after the
			// server has received the complete request and answered.
			if progress != nil {
				progress(SpeedProgress{Phase: "upload", CompletedBytes: done, TotalBytes: uploadBytes, DownloadMbps: down})
			}
		},
	}
	ureq.Body = io.NopCloser(uploadReader)
	uresp, err := client.Do(ureq)
	if err != nil {
		return down, 0, fmt.Errorf("upload: %w", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(uresp.Body, 4096))
	_ = uresp.Body.Close()
	if uploadReader.read != uploadBytes { return down, 0, fmt.Errorf("upload: sent %d/%d bytes", uploadReader.read, uploadBytes) }
	upElapsed := time.Since(uploadStart).Seconds()
	if uresp.StatusCode < 200 || uresp.StatusCode >= 300 {
		return down, 0, fmt.Errorf("upload: HTTP %d", uresp.StatusCode)
	}
	if upElapsed <= 0 {
		return down, 0, fmt.Errorf("upload: invalid duration")
	}
	// Measure the full request round-trip: unlike counting Body.Read calls,
	// this includes the time until the upload endpoint has accepted the body.
	up := float64(uploadBytes) * 8 / upElapsed / 1e6
	if progress != nil {
		progress(SpeedProgress{Phase: "upload complete", CompletedBytes: uploadBytes, TotalBytes: uploadBytes, DownloadMbps: down, UploadMbps: up})
	}
	return down, up, nil
}


// NetworkQuality contains Cloudflare-edge HTTP measurements used by the
// experimental gaming-quality stage. Packet loss is deliberately not included:
// Cloudflare's public HTTP endpoints do not expose UDP loss without TURN credentials.
type NetworkQuality struct {
	LatencyMS                float64
	JitterMS                 float64
	DownloadLoadedLatencyMS  float64
	DownloadLoadedJitterMS   float64
	UploadLoadedLatencyMS    float64
	UploadLoadedJitterMS     float64
}

// CloudflareNetworkQualityTest follows the public Cloudflare Speed Test model:
// zero-byte requests estimate unloaded and loaded latency, and jitter is the
// average absolute delta between consecutive samples. Unlike the browser engine,
// Go measures request-to-response-headers duration directly and subtracts the
// Cloudflare Server-Timing duration when it is available.
func CloudflareNetworkQualityTest(
	ctx context.Context,
	client *http.Client,
	downloadURL, uploadURL string,
	downloadBytes, uploadBytes int64,
	latencySamples int,
	sampleInterval time.Duration,
) (NetworkQuality, error) {
	var result NetworkQuality
	if client == nil {
		return result, fmt.Errorf("network quality test requires an HTTP client")
	}
	if latencySamples < 3 || latencySamples > 100 {
		return result, fmt.Errorf("latency_samples must be between 3 and 100")
	}
	if sampleInterval <= 0 {
		return result, fmt.Errorf("sample interval must be positive")
	}
	if downloadBytes < 64*1024 || uploadBytes < 64*1024 || downloadBytes > 100<<20 || uploadBytes > 100<<20 {
		return result, fmt.Errorf("gaming download/upload payloads must be between 64 KiB and 100 MiB")
	}
	latencyURL, err := cloudflareBytesURL(downloadURL, 0)
	if err != nil {
		return result, fmt.Errorf("Cloudflare latency URL: %w", err)
	}
	downloadURL, err = cloudflareBytesURL(downloadURL, downloadBytes)
	if err != nil {
		return result, fmt.Errorf("Cloudflare download URL: %w", err)
	}
	if err := validateTestURL(uploadURL); err != nil {
		return result, fmt.Errorf("Cloudflare upload URL: %w", err)
	}

	idle, err := collectCloudflareLatency(ctx, client, latencyURL, latencySamples, sampleInterval)
	if err != nil {
		return result, fmt.Errorf("unloaded latency: %w", err)
	}
	result.LatencyMS = medianFloat64(idle)
	result.JitterMS = meanAbsoluteDelta(idle)

	downSamples, err := collectCloudflareLatencyDuring(ctx, client, latencyURL, sampleInterval, func(loadCtx context.Context) error {
		return cloudflareDownloadProbe(loadCtx, client, downloadURL, downloadBytes)
	})
	if err != nil {
		return result, fmt.Errorf("download-loaded latency: %w", err)
	}
	result.DownloadLoadedLatencyMS = medianFloat64(downSamples)
	result.DownloadLoadedJitterMS = meanAbsoluteDelta(downSamples)

	uploadSamples, err := collectCloudflareLatencyDuring(ctx, client, latencyURL, sampleInterval, func(loadCtx context.Context) error {
		return cloudflareUploadProbe(loadCtx, client, uploadURL, uploadBytes)
	})
	if err != nil {
		return result, fmt.Errorf("upload-loaded latency: %w", err)
	}
	result.UploadLoadedLatencyMS = medianFloat64(uploadSamples)
	result.UploadLoadedJitterMS = meanAbsoluteDelta(uploadSamples)
	return result, nil
}

func cloudflareBytesURL(raw string, numBytes int64) (string, error) {
	raw = strings.ReplaceAll(strings.TrimSpace(raw), "{bytes}", strconv.FormatInt(numBytes, 10))
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("must be an absolute HTTP(S) URL")
	}
	query := u.Query()
	query.Set("bytes", strconv.FormatInt(numBytes, 10))
	u.RawQuery = query.Encode()
	return u.String(), nil
}

var cloudflareRequestDuration = regexp.MustCompile(`(?i)(?:^|,\s*)cfReq(?:uest)?Dur(?:ation)?;\s*dur=([0-9.]+)`)
var cloudflareSpeedDuration = regexp.MustCompile(`(?i)(?:^|,\\s*)cfSpeed[a-zA-Z]*;\s*dur=([0-9.]+)`)

func cloudflareServerDuration(header string) float64 {
	if match := cloudflareRequestDuration.FindStringSubmatch(header); len(match) == 2 {
		if value, err := strconv.ParseFloat(match[1], 64); err == nil && value > 0.01 {
			return value
		}
	}
	total := 0.0
	for _, match := range cloudflareSpeedDuration.FindAllStringSubmatch(header, -1) {
		if len(match) != 2 {
			continue
		}
		if value, err := strconv.ParseFloat(match[1], 64); err == nil {
			total += value
		}
	}
	if total > 0.01 {
		return total
	}
	return 0
}

func cloudflareLatencySample(ctx context.Context, client *http.Client, target string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "SubWatch/experimental Cloudflare network quality")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	elapsed := time.Since(start)
	serverDuration := time.Duration(cloudflareServerDuration(resp.Header.Get("Server-Timing")) * float64(time.Millisecond))
	_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if readErr != nil {
		return 0, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("HTTP %s", resp.Status)
	}
	elapsed -= serverDuration
	if elapsed < 0 {
		elapsed = 0
	}
	return float64(elapsed) / float64(time.Millisecond), nil
}

func collectCloudflareLatency(ctx context.Context, client *http.Client, target string, count int, interval time.Duration) ([]float64, error) {
	samples := make([]float64, 0, count)
	for i := 0; i < count; i++ {
		sample, err := cloudflareLatencySample(ctx, client, target)
		if err != nil {
			return nil, fmt.Errorf("sample %d/%d: %w", i+1, count, err)
		}
		samples = append(samples, sample)
		if i+1 < count {
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return samples, nil
}

func collectCloudflareLatencyDuring(ctx context.Context, client *http.Client, target string, interval time.Duration, transfer func(context.Context) error) ([]float64, error) {
	loadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	transferDone := make(chan error, 1)
	go func() {
		transferDone <- transfer(loadCtx)
	}()

	samples := make([]float64, 0, 16)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case err := <-transferDone:
			if err != nil {
				return nil, err
			}
			if len(samples) < 2 {
				return nil, fmt.Errorf("only %d latency samples collected while traffic was active; increase gaming_test payload size or reduce sample_interval", len(samples))
			}
			return samples, nil
		case <-ticker.C:
			sample, err := cloudflareLatencySample(loadCtx, client, target)
			if err != nil {
				return nil, fmt.Errorf("loaded sample: %w", err)
			}
			samples = append(samples, sample)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func cloudflareDownloadProbe(ctx context.Context, client *http.Client, target string, expectedBytes int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "SubWatch/experimental Cloudflare network quality")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	received, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		return err
	}
	if received < expectedBytes {
		return fmt.Errorf("received %d/%d bytes", received, expectedBytes)
	}
	return nil
}

func cloudflareUploadProbe(ctx context.Context, client *http.Client, target string, numBytes int64) error {
	payload := bytes.Repeat([]byte{0x5a}, int(numBytes))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "SubWatch/experimental Cloudflare network quality")
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", strconv.FormatInt(numBytes, 10))
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	return nil
}

func medianFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

func meanAbsoluteDelta(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	total := 0.0
	for i := 1; i < len(values); i++ {
		delta := values[i] - values[i-1]
		if delta < 0 {
			delta = -delta
		}
		total += delta
	}
	return total / float64(len(values)-1)
}

func enforceTransferWindow(ctx context.Context, cancel context.CancelFunc, firstByte <-chan struct{}, firstTimeout, maxDuration time.Duration) {
	first := time.NewTimer(firstTimeout)
	defer first.Stop()
	select {
	case <-ctx.Done(): return
	case <-first.C: cancel(); return
	case <-firstByte:
	}
	measurement := time.NewTimer(maxDuration)
	defer measurement.Stop()
	select { case <-ctx.Done(): return; case <-measurement.C: cancel() }
}

type uploadProgressReader struct {
	reader     io.Reader
	total      int64
	read       int64
	started    time.Time
	lastBytes  int64
	lastReport time.Time
	interval time.Duration
	firstByteAt time.Time
	onProgress func(int64, time.Duration)
}

func (r *uploadProgressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.read += int64(n)
		now := time.Now()
		if r.firstByteAt.IsZero() { r.firstByteAt = now }
		if r.onProgress != nil && (r.lastReport.IsZero() || now.Sub(r.lastReport) >= r.interval || r.read == r.total) {
			r.onProgress(r.read, now.Sub(r.started))
			r.lastBytes, r.lastReport = r.read, now
		}
	}
	return n, err
}

func validateTestURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("must be an absolute HTTP(S) URL")
	}
	return nil
}

var geminiTrue = regexp.MustCompile(`(?:^|[^0-9])45631641(?:\\?":|,null,)true`)
var geminiFalse = regexp.MustCompile(`(?:^|[^0-9])45631641(?:\\?":|,null,)false`)
var geminiRegion = regexp.MustCompile(`,2,1,200,"([A-Z]{3})"`)

func GeminiTest(ctx context.Context, client *http.Client) (status, note, region string, err error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, "https://gemini.google.com/app?hl=en", nil)
	if e != nil {
		return "failed", "request creation", "", e
	}
	req.Header.Set("Cookie", "SOCS=CAI")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br, zstd")
	req.Header.Set("Sec-Ch-Ua", `"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"`)
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", `"Windows"`)
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	resp, e := client.Do(req)
	if e != nil {
		return "failed", e.Error(), "", e
	}
	defer resp.Body.Close()
	body, readErr := readGeminiBody(resp, 1572864)
	if readErr != nil {
		return "failed", "response read error", "", readErr
	}
	if m := geminiRegion.FindSubmatch(body); len(m) > 1 {
		region = string(m[1])
	}
	finalURL := ""
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL.String()
	}
	status, note, classifiedRegion := ClassifyGemini(resp.StatusCode, finalURL, body)
	if region == "" {
		region = classifiedRegion
	}
	return status, note, region, nil
}

// ClassifyGemini mirrors Throne's public, signed-out page classifier. It does not
// authenticate or infer account-specific eligibility.
func ClassifyGemini(httpStatus int, finalURL string, body []byte) (status, note, region string) {
	if m := geminiRegion.FindSubmatch(body); len(m) > 1 {
		region = string(m[1])
	}
	if httpStatus >= 200 && httpStatus < 300 && strings.HasPrefix(finalURL, "https://consent.") {
		return "unknown", "Consent page", region
	}
	if httpStatus >= 200 && httpStatus < 300 && strings.HasPrefix(finalURL, "https://gemini.google.com/") {
		if geminiTrue.Match(body) {
			return "available", "", region
		}
		if geminiFalse.Match(body) {
			return "partial", "Refused when signed out; a signed-in account can still work", region
		}
		return "unknown", "No availability flag on the page", region
	}
	if httpStatus == http.StatusForbidden || httpStatus == http.StatusTooManyRequests {
		return "blocked", fmt.Sprintf("HTTP %d", httpStatus), region
	}
	return "unknown", fmt.Sprintf("Unexpected response (HTTP %d)", httpStatus), region
}

type IPDetails struct {
	IP, Country, ASN, Org, Type string
	Flags                       map[string]bool
}

func LookupIP(ctx context.Context, client *http.Client) IPDetails {
	var result IPDetails
	result.Flags = map[string]bool{}
	for _, provider := range []struct {
		name, url string
		paths     map[string]string
	}{
		{"iplocate", "https://iplocate.io/api/lookup/", map[string]string{"ip": "ip", "country": "country_code", "asn": "asn.asn", "org": "asn.name", "type": "asn.type", "vpn": "privacy.is_vpn", "proxy": "privacy.is_proxy", "tor": "privacy.is_tor", "hosting": "privacy.is_hosting"}},
		{"ipquery", "https://api.ipquery.io/?format=json", map[string]string{"ip": "ip", "country": "location.country_code", "asn": "isp.asn", "org": "isp.org", "type": "isp.type", "vpn": "risk.is_vpn", "proxy": "risk.is_proxy", "tor": "risk.is_tor", "hosting": "risk.is_datacenter"}},
	} {
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, provider.url, nil)
		if e != nil {
			continue
		}
		req.Header.Set("User-Agent", "SubWatch/0.1")
		resp, e := client.Do(req)
		if e != nil {
			continue
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			continue
		}
		var root map[string]any
		if json.Unmarshal(data, &root) != nil {
			continue
		}
		for field, path := range provider.paths {
			v := jsonPath(root, path)
			switch field {
			case "ip":
				if result.IP == "" {
					result.IP = asString(v)
				}
			case "country":
				if result.Country == "" {
					result.Country = asString(v)
				}
			case "asn":
				if result.ASN == "" {
					result.ASN = asString(v)
				}
			case "org":
				if result.Org == "" {
					result.Org = asString(v)
				}
			case "type":
				if result.Type == "" {
					result.Type = asString(v)
				}
			default:
				if b, ok := v.(bool); ok {
					result.Flags[field] = b
				}
			}
		}
	}
	return result
}
func jsonPath(root map[string]any, path string) any {
	var cur any = root
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[part]
	}
	return cur
}
func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return fmt.Sprintf("%.0f", x)
	case json.Number:
		return string(x)
	default:
		return ""
	}
}

func NodeContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}
func ApplyIP(n *sub.Node, info IPDetails) {
	n.ExitIP = info.IP
	n.Country = info.Country
	n.ASN = info.ASN
	n.Org = info.Org
	n.IPType = info.Type
	n.IPFlags = info.Flags
}
